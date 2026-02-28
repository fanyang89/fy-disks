#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace fyd {

struct SegmentInfo {
    uint64_t    seg_idx;
    std::string backend;   // "local" or "remote:<node>"
    std::string path;      // local: file path; remote: addr
};

struct DiskConfig {
    std::string              name;
    uint64_t                 blocks;
    uint32_t                 block_size;
    uint64_t                 seg_blocks;  // blocks per segment
    std::string              iqn;
    std::vector<SegmentInfo> segments;
};

class Meta {
public:
    explicit Meta(const std::string& db_path);
    ~Meta();

    // Create the fyd.disks and fyd.segments tables if they don't exist.
    void init_schema();

    // Return all disks together with their segment lists.
    std::vector<DiskConfig> list_disks();

    // Persist a new disk record (rows in fyd.disks + fyd.segments).
    void add_disk(const DiskConfig& d);

    // Remove a disk and all its segments from the metadata store.
    void remove_disk(const std::string& name);

private:
    struct Impl;
    Impl* impl_;
};

} // namespace fyd
