#include "meta.hpp"
#include "startup.h"

#include <CLI/CLI.hpp>

extern "C" {
#include "spdk/event.h"
#include "spdk/log.h"
}

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// ── Build the C startup context from the C++ DiskConfig list ────────────────

static fyd_app_ctx build_ctx(
    const std::vector<fyd::DiskConfig>& disks,
    const std::string& addr,
    uint16_t port)
{
    fyd_app_ctx ctx{};

    if (disks.size() > FYD_MAX_DISKS) {
        throw std::runtime_error("Too many disks: max " +
                                 std::to_string(FYD_MAX_DISKS));
    }

    snprintf(ctx.addr, sizeof(ctx.addr), "%s", addr.c_str());
    ctx.port      = port;
    ctx.num_disks = static_cast<uint32_t>(disks.size());

    uint32_t max_segs_per_disk =
        static_cast<uint32_t>(FYD_MAX_SEGS / FYD_MAX_DISKS);

    for (uint32_t di = 0; di < ctx.num_disks; di++) {
        const fyd::DiskConfig& d = disks[di];
        fyd_disk_info& info = ctx.disks[di];

        if (d.segments.size() > max_segs_per_disk) {
            throw std::runtime_error("Disk '" + d.name + "' has too many segments");
        }

        // These string pointers refer into the disks vector, which lives
        // for the duration of main() / spdk_app_start(), so this is safe.
        info.name       = d.name.c_str();
        info.blocks     = d.blocks;
        info.block_size = d.block_size;
        info.seg_blocks = d.seg_blocks;
        info.iqn        = d.iqn.c_str();
        info.num_segs   = static_cast<uint32_t>(d.segments.size());

        for (uint32_t si = 0; si < info.num_segs; si++) {
            info.segs[si].seg_idx = d.segments[si].seg_idx;
            info.segs[si].backend = d.segments[si].backend.c_str();
            info.segs[si].path    = d.segments[si].path.c_str();
        }
    }

    return ctx;
}

// ── Optional: create a default disk when the metadata is empty ──────────────

static void maybe_create_default_disk(
    fyd::Meta& meta,
    uint64_t    disk_size_gb,
    uint64_t    seg_size_mb,
    uint32_t    block_size,
    const std::string& data_dir)
{
    auto disks = meta.list_disks();
    if (!disks.empty()) return;

    uint64_t total_bytes  = disk_size_gb * 1024ULL * 1024 * 1024;
    uint64_t seg_bytes    = seg_size_mb  * 1024ULL * 1024;
    uint64_t total_blocks = total_bytes  / block_size;
    uint64_t seg_blocks   = seg_bytes    / block_size;
    uint64_t num_segs     = (total_blocks + seg_blocks - 1) / seg_blocks;

    fyd::DiskConfig d;
    d.name       = "disk0";
    d.blocks     = total_blocks;
    d.block_size = block_size;
    d.seg_blocks = seg_blocks;
    d.iqn        = "iqn.2024-01.io.fyd:disk0";

    // Ensure data directory exists
    fs::create_directories(data_dir + "/disk0");

    for (uint64_t i = 0; i < num_segs; i++) {
        fyd::SegmentInfo s;
        s.seg_idx = i;
        s.backend = "local";
        s.path    = data_dir + "/disk0/seg_" + std::to_string(i) + ".img";
        d.segments.push_back(std::move(s));
    }

    meta.add_disk(d);
    fprintf(stderr, "Created default disk0: %" PRIu64 " GB, "
            "%" PRIu64 " segments x %" PRIu64 " MB\n",
            disk_size_gb, num_segs, seg_size_mb);
}

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
    CLI::App app{"fy-iscsi: iSCSI storage server"};

    std::string meta_path    = "/var/lib/fyd/meta.db";
    std::string addr         = "0.0.0.0";
    uint16_t    port         = 3260;
    uint64_t    seg_size_mb  = 64;
    uint64_t    disk_size_gb = 1;
    uint32_t    block_size   = 4096;
    std::string data_dir     = "/var/lib/fyd";
    bool        create_default = false;

    app.add_option("--meta-path",     meta_path,      "ChDB metadata path");
    app.add_option("--addr",          addr,           "iSCSI listen address");
    app.add_option("--port",          port,           "iSCSI listen port");
    app.add_option("--seg-size-mb",   seg_size_mb,    "Segment size (MB)");
    app.add_option("--disk-size-gb",  disk_size_gb,   "Default disk size (GB)");
    app.add_option("--block-size",    block_size,     "Block size in bytes");
    app.add_option("--data-dir",      data_dir,       "Backing file directory");
    app.add_flag("--create-default",  create_default,
                 "Create a default disk0 if no disks exist");

    CLI11_PARSE(app, argc, argv);

    // Initialise metadata
    fyd::Meta meta(meta_path);
    meta.init_schema();

    if (create_default) {
        try {
            maybe_create_default_disk(meta, disk_size_gb, seg_size_mb,
                                      block_size, data_dir);
        } catch (const std::exception& e) {
            fprintf(stderr, "Failed to create default disk: %s\n", e.what());
            return 1;
        }
    }

    // Load disk configurations
    std::vector<fyd::DiskConfig> disks;
    try {
        disks = meta.list_disks();
    } catch (const std::exception& e) {
        fprintf(stderr, "Failed to load disk metadata: %s\n", e.what());
        return 1;
    }

    if (disks.empty()) {
        fprintf(stderr, "No disks configured. Use --create-default or add "
                "disks to the metadata store.\n");
        return 1;
    }

    // Build the C context (must outlive spdk_app_start)
    fyd_app_ctx ctx{};
    try {
        ctx = build_ctx(disks, addr, port);
    } catch (const std::exception& e) {
        fprintf(stderr, "Failed to build app context: %s\n", e.what());
        return 1;
    }

    // Launch SPDK
    struct spdk_app_opts opts = {};
    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name        = "fy-iscsi";
    opts.print_level = SPDK_LOG_NOTICE;

    int rc = spdk_app_start(&opts, fyd_startup, &ctx);
    spdk_app_fini();

    return rc;
}
