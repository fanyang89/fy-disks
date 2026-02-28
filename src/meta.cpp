#include "meta.hpp"
#include "chdb.hpp"

#include <sstream>
#include <stdexcept>
#include <string>

namespace fyd {

// ── Pimpl ────────────────────────────────────────────────────────────────────

struct Meta::Impl {
    CHDB::Connection conn;

    explicit Impl(const std::string& db_path) : conn(db_path) {}
};

// ── Construction ─────────────────────────────────────────────────────────────

Meta::Meta(const std::string& db_path) : impl_(new Impl(db_path)) {}

Meta::~Meta() { delete impl_; }

// ── Schema ───────────────────────────────────────────────────────────────────

void Meta::init_schema()
{
    impl_->conn.query(
        "CREATE DATABASE IF NOT EXISTS fyd"
    ).throw_if_error();

    impl_->conn.query(
        "CREATE TABLE IF NOT EXISTS fyd.disks ("
        "  name        String,"
        "  blocks      UInt64,"
        "  block_size  UInt32,"
        "  seg_blocks  UInt64,"
        "  iqn         String"
        ") ENGINE = MergeTree() ORDER BY name"
    ).throw_if_error();

    impl_->conn.query(
        "CREATE TABLE IF NOT EXISTS fyd.segments ("
        "  disk_name   String,"
        "  seg_idx     UInt64,"
        "  backend     String,"
        "  path        String"
        ") ENGINE = MergeTree() ORDER BY (disk_name, seg_idx)"
    ).throw_if_error();
}

// ── List ─────────────────────────────────────────────────────────────────────

// Parse a single TSV line into fields split by '\t'.
static std::vector<std::string> split_tsv(const std::string& line)
{
    std::vector<std::string> fields;
    std::istringstream ss(line);
    std::string field;
    while (std::getline(ss, field, '\t')) {
        fields.push_back(field);
    }
    return fields;
}

// Iterate over non-empty lines returned by a TabSeparated query.
static std::vector<std::vector<std::string>> parse_tsv(const std::string& data)
{
    std::vector<std::vector<std::string>> rows;
    std::istringstream ss(data);
    std::string line;
    while (std::getline(ss, line)) {
        if (!line.empty()) {
            rows.push_back(split_tsv(line));
        }
    }
    return rows;
}

std::vector<DiskConfig> Meta::list_disks()
{
    // Fetch all disks
    auto disk_result = impl_->conn.query(
        "SELECT name, blocks, block_size, seg_blocks, iqn "
        "FROM fyd.disks ORDER BY name"
    );
    disk_result.throw_if_error();

    std::vector<DiskConfig> disks;
    auto disk_rows = parse_tsv(disk_result.str());
    for (auto& row : disk_rows) {
        if (row.size() < 5) continue;
        DiskConfig d;
        d.name       = row[0];
        d.blocks     = std::stoull(row[1]);
        d.block_size = static_cast<uint32_t>(std::stoul(row[2]));
        d.seg_blocks = std::stoull(row[3]);
        d.iqn        = row[4];
        disks.push_back(std::move(d));
    }

    if (disks.empty()) return disks;

    // Fetch all segments in one query
    auto seg_result = impl_->conn.query(
        "SELECT disk_name, seg_idx, backend, path "
        "FROM fyd.segments ORDER BY disk_name, seg_idx"
    );
    seg_result.throw_if_error();

    auto seg_rows = parse_tsv(seg_result.str());
    for (auto& row : seg_rows) {
        if (row.size() < 4) continue;
        const std::string& dname = row[0];
        for (auto& d : disks) {
            if (d.name == dname) {
                SegmentInfo s;
                s.seg_idx = std::stoull(row[1]);
                s.backend = row[2];
                s.path    = row[3];
                d.segments.push_back(std::move(s));
                break;
            }
        }
    }

    return disks;
}

// ── Add ──────────────────────────────────────────────────────────────────────

// Escape single quotes inside a SQL string value.
static std::string sql_escape(const std::string& s)
{
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '\'') out += '\'';
        out += c;
    }
    return out;
}

void Meta::add_disk(const DiskConfig& d)
{
    std::ostringstream ss;
    ss << "INSERT INTO fyd.disks (name, blocks, block_size, seg_blocks, iqn) VALUES ("
       << "'" << sql_escape(d.name) << "',"
       << d.blocks << ","
       << d.block_size << ","
       << d.seg_blocks << ","
       << "'" << sql_escape(d.iqn) << "'"
       << ")";
    impl_->conn.query(ss.str()).throw_if_error();

    for (const auto& seg : d.segments) {
        std::ostringstream ss2;
        ss2 << "INSERT INTO fyd.segments (disk_name, seg_idx, backend, path) VALUES ("
            << "'" << sql_escape(d.name) << "',"
            << seg.seg_idx << ","
            << "'" << sql_escape(seg.backend) << "',"
            << "'" << sql_escape(seg.path) << "'"
            << ")";
        impl_->conn.query(ss2.str()).throw_if_error();
    }
}

// ── Remove ───────────────────────────────────────────────────────────────────

void Meta::remove_disk(const std::string& name)
{
    std::string esc = sql_escape(name);
    impl_->conn.query(
        "ALTER TABLE fyd.disks DELETE WHERE name='" + esc + "'"
    ).throw_if_error();
    impl_->conn.query(
        "ALTER TABLE fyd.segments DELETE WHERE disk_name='" + esc + "'"
    ).throw_if_error();
}

} // namespace fyd
