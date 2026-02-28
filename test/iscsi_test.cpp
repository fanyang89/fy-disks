/*
 * iscsi_test.cpp — libiscsi-based integration tests for fy-disks
 *
 * Uses the synchronous libiscsi API; no kernel iSCSI initiator required.
 *
 * Usage:
 *   iscsi_test --addr 127.0.0.1 --port 3260 [--iqn <iqn>] --case <basic|cross_segment>
 */

#include <iscsi/iscsi.h>
#include <iscsi/scsi-lowlevel.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>

// ── RAII wrapper around iscsi_context ────────────────────────────────────────

struct IscsiSession {
    struct iscsi_context *ctx = nullptr;

    IscsiSession(const std::string& initiator_name,
                 const std::string& target_name,
                 const std::string& addr,
                 uint16_t port)
    {
        std::string portal = addr + ":" + std::to_string(port);
        std::string url    = "iscsi://" + addr + ":" + std::to_string(port)
                           + "/" + target_name + "/0";

        ctx = iscsi_create_context(initiator_name.c_str());
        if (!ctx) throw std::runtime_error("iscsi_create_context failed");

        if (iscsi_set_targetname(ctx, target_name.c_str()) != 0) {
            iscsi_destroy_context(ctx);
            throw std::runtime_error("iscsi_set_targetname failed");
        }

        if (iscsi_set_session_type(ctx, ISCSI_SESSION_NORMAL) != 0) {
            iscsi_destroy_context(ctx);
            throw std::runtime_error("iscsi_set_session_type failed");
        }

        if (iscsi_connect_sync(ctx, portal.c_str()) != 0) {
            std::string err = iscsi_get_error(ctx);
            iscsi_destroy_context(ctx);
            throw std::runtime_error("connect failed: " + err);
        }

        if (iscsi_login_sync(ctx) != 0) {
            std::string err = iscsi_get_error(ctx);
            iscsi_destroy_context(ctx);
            throw std::runtime_error("login failed: " + err);
        }
    }

    ~IscsiSession() {
        if (ctx) {
            iscsi_logout_sync(ctx);
            iscsi_destroy_context(ctx);
        }
    }

    IscsiSession(const IscsiSession&) = delete;
    IscsiSession& operator=(const IscsiSession&) = delete;
};

// ── Assertion helper ─────────────────────────────────────────────────────────

static void check(bool cond, const char* msg) {
    if (!cond) {
        fprintf(stderr, "FAIL: %s\n", msg);
        exit(1);
    }
}

// ── Test: basic I/O ──────────────────────────────────────────────────────────

static void test_basic(const std::string& addr, uint16_t port,
                       const std::string& target_iqn)
{
    fprintf(stdout, "=== BasicIO ===\n");

    IscsiSession s("iqn.2024-01.io.fyd:test-init", target_iqn, addr, port);

    // INQUIRY
    struct scsi_task *task = iscsi_inquiry_sync(s.ctx, 0, 0, 0, 64);
    check(task != nullptr, "INQUIRY failed");
    check(task->status == SCSI_STATUS_GOOD, "INQUIRY: bad status");
    scsi_free_scsi_task(task);
    fprintf(stdout, "  INQUIRY: OK\n");

    // READ CAPACITY 16
    task = iscsi_readcapacity16_sync(s.ctx, 0);
    check(task != nullptr, "READCAPACITY16 failed");
    check(task->status == SCSI_STATUS_GOOD, "READCAPACITY16: bad status");

    struct scsi_readcapacity16 *rc16 =
        (struct scsi_readcapacity16 *)scsi_datain_unmarshall(task);
    check(rc16 != nullptr, "READCAPACITY16: unmarshall failed");

    uint64_t total_lbas  = rc16->returned_lba + 1;
    uint32_t block_len   = rc16->block_length;
    scsi_free_scsi_task(task);

    fprintf(stdout, "  READCAPACITY16: %" PRIu64 " blocks x %u bytes\n",
            total_lbas, block_len);
    check(block_len > 0 && block_len <= 65536, "unexpected block length");
    check(total_lbas > 0, "disk appears empty");

    // WRITE 4 blocks at LBA 0
    const int num_blocks = 4;
    uint32_t data_len = num_blocks * block_len;
    unsigned char *write_buf = (unsigned char *)malloc(data_len);
    check(write_buf != nullptr, "malloc failed");

    // Fill with recognizable pattern
    for (uint32_t i = 0; i < data_len; i++) {
        write_buf[i] = (unsigned char)(i & 0xFF);
    }

    task = iscsi_write10_sync(s.ctx, 0,       /* lun 0 */
                              write_buf, data_len,
                              0,               /* LBA */
                              0, 0, 0, 0,      /* wrprotect, dpo, fua, fua_nv */
                              block_len);
    check(task != nullptr, "WRITE10 failed");
    check(task->status == SCSI_STATUS_GOOD, "WRITE10: bad status");
    scsi_free_scsi_task(task);
    fprintf(stdout, "  WRITE10 (%d blocks at LBA 0): OK\n", num_blocks);

    // READ back and verify
    unsigned char *read_buf = (unsigned char *)malloc(data_len);
    check(read_buf != nullptr, "malloc failed");

    task = iscsi_read10_sync(s.ctx, 0, 0, data_len,
                             block_len, 0, 0, 0, 0);
    check(task != nullptr, "READ10 failed");
    check(task->status == SCSI_STATUS_GOOD, "READ10: bad status");
    check(task->datain.size == data_len, "READ10: unexpected data length");
    check(memcmp(task->datain.data, write_buf, data_len) == 0,
          "READ10: data mismatch");
    scsi_free_scsi_task(task);
    fprintf(stdout, "  READ10 + verify: OK\n");

    free(write_buf);
    free(read_buf);

    fprintf(stdout, "=== BasicIO PASSED ===\n");
}

// ── Test: cross-segment I/O ──────────────────────────────────────────────────
//
// The plan configures seg_blocks = 64 MB / block_size.
// We write starting 2 blocks before the first segment boundary, spanning
// into the second segment, and verify the data comes back intact.

static void test_cross_segment(const std::string& addr, uint16_t port,
                                const std::string& target_iqn,
                                uint64_t seg_size_mb)
{
    fprintf(stdout, "=== CrossSegment ===\n");

    IscsiSession s("iqn.2024-01.io.fyd:test-init", target_iqn, addr, port);

    // Get geometry
    struct scsi_task *task = iscsi_readcapacity16_sync(s.ctx, 0);
    check(task != nullptr, "READCAPACITY16 failed");
    check(task->status == SCSI_STATUS_GOOD, "READCAPACITY16: bad status");

    struct scsi_readcapacity16 *rc16 =
        (struct scsi_readcapacity16 *)scsi_datain_unmarshall(task);
    check(rc16 != nullptr, "READCAPACITY16: unmarshall failed");

    uint64_t total_lbas = rc16->returned_lba + 1;
    uint32_t block_len  = rc16->block_length;
    scsi_free_scsi_task(task);

    uint64_t seg_blocks = (seg_size_mb * 1024ULL * 1024) / block_len;

    // Write 4 blocks straddling the first segment boundary
    // (2 blocks before boundary, 2 blocks after)
    if (total_lbas < seg_blocks + 2) {
        fprintf(stdout, "  SKIP: disk too small to test cross-segment "
                "(need %" PRIu64 " blocks, have %" PRIu64 ")\n",
                seg_blocks + 2, total_lbas);
        return;
    }

    uint64_t start_lba  = seg_blocks - 2;   /* 2 blocks before boundary */
    const int num_blocks = 4;
    uint32_t data_len   = (uint32_t)(num_blocks * block_len);

    unsigned char *write_buf = (unsigned char *)malloc(data_len);
    check(write_buf != nullptr, "malloc failed");
    for (uint32_t i = 0; i < data_len; i++) {
        write_buf[i] = (unsigned char)((i + 0xAB) & 0xFF);
    }

    task = iscsi_write10_sync(s.ctx, 0,
                              write_buf, data_len,
                              (uint32_t)start_lba,
                              0, 0, 0, 0,
                              block_len);
    check(task != nullptr, "WRITE10 (cross-seg) failed");
    check(task->status == SCSI_STATUS_GOOD, "WRITE10 (cross-seg): bad status");
    scsi_free_scsi_task(task);
    fprintf(stdout, "  WRITE10 (%d blocks at LBA %" PRIu64 " crossing seg): OK\n",
            num_blocks, start_lba);

    // Read back
    task = iscsi_read10_sync(s.ctx, 0,
                             (uint32_t)start_lba,
                             data_len, block_len,
                             0, 0, 0, 0);
    check(task != nullptr, "READ10 (cross-seg) failed");
    check(task->status == SCSI_STATUS_GOOD, "READ10 (cross-seg): bad status");
    check(task->datain.size == data_len, "READ10 (cross-seg): unexpected size");
    check(memcmp(task->datain.data, write_buf, data_len) == 0,
          "READ10 (cross-seg): data mismatch");
    scsi_free_scsi_task(task);
    fprintf(stdout, "  READ10 + verify: OK\n");

    free(write_buf);

    fprintf(stdout, "=== CrossSegment PASSED ===\n");
}

// ── Discover the first target on the portal ──────────────────────────────────

static std::string discover_target(const std::string& addr, uint16_t port)
{
    struct iscsi_context *ctx = iscsi_create_context("iqn.2024-01.io.fyd:discovery");
    if (!ctx) throw std::runtime_error("iscsi_create_context failed");

    if (iscsi_set_session_type(ctx, ISCSI_SESSION_DISCOVERY) != 0) {
        iscsi_destroy_context(ctx);
        throw std::runtime_error("iscsi_set_session_type failed");
    }

    std::string portal = addr + ":" + std::to_string(port);

    if (iscsi_connect_sync(ctx, portal.c_str()) != 0) {
        std::string err = iscsi_get_error(ctx);
        iscsi_destroy_context(ctx);
        throw std::runtime_error("discovery connect failed: " + err);
    }

    if (iscsi_login_sync(ctx) != 0) {
        std::string err = iscsi_get_error(ctx);
        iscsi_destroy_context(ctx);
        throw std::runtime_error("discovery login failed: " + err);
    }

    struct iscsi_discovery_address *addrs =
        iscsi_discovery_sync(ctx, portal.c_str(), nullptr);

    if (!addrs) {
        std::string err = iscsi_get_error(ctx);
        iscsi_logout_sync(ctx);
        iscsi_destroy_context(ctx);
        throw std::runtime_error("discovery failed: " + err);
    }

    std::string target_iqn = addrs->target_name;
    iscsi_free_discovery_data(ctx, addrs);

    iscsi_logout_sync(ctx);
    iscsi_destroy_context(ctx);

    return target_iqn;
}

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
    std::string addr        = "127.0.0.1";
    uint16_t    port        = 3260;
    std::string test_case   = "basic";
    std::string target_iqn  = "";
    uint64_t    seg_size_mb = 64;

    // Simple argument parser (no external deps)
    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "--addr" && i + 1 < argc) {
            addr = argv[++i];
        } else if (std::string(argv[i]) == "--port" && i + 1 < argc) {
            port = (uint16_t)std::stoi(argv[++i]);
        } else if (std::string(argv[i]) == "--case" && i + 1 < argc) {
            test_case = argv[++i];
        } else if (std::string(argv[i]) == "--iqn" && i + 1 < argc) {
            target_iqn = argv[++i];
        } else if (std::string(argv[i]) == "--seg-size-mb" && i + 1 < argc) {
            seg_size_mb = std::stoull(argv[++i]);
        }
    }

    // Auto-discover target IQN if not provided
    if (target_iqn.empty()) {
        try {
            target_iqn = discover_target(addr, port);
            fprintf(stdout, "Discovered target: %s\n", target_iqn.c_str());
        } catch (const std::exception& e) {
            fprintf(stderr, "Discovery failed: %s\n", e.what());
            return 1;
        }
    }

    try {
        if (test_case == "basic") {
            test_basic(addr, port, target_iqn);
        } else if (test_case == "cross_segment") {
            test_cross_segment(addr, port, target_iqn, seg_size_mb);
        } else {
            fprintf(stderr, "Unknown test case: %s\n", test_case.c_str());
            return 1;
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "Test failed: %s\n", e.what());
        return 1;
    }

    return 0;
}
