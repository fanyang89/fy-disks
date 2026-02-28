#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

/* Maximum disks and segments supported in a single startup context */
#define FYD_MAX_DISKS    64
#define FYD_MAX_SEGS     4096

/* Per-segment info passed from C++ layer to startup.c */
struct fyd_seg_info {
    uint64_t    seg_idx;
    const char *backend;   /* "local" or "remote:<node>" */
    const char *path;      /* backing file path */
};

/* Per-disk info */
struct fyd_disk_info {
    const char         *name;
    uint64_t            blocks;
    uint32_t            block_size;
    uint64_t            seg_blocks;
    const char         *iqn;
    uint32_t            num_segs;
    struct fyd_seg_info segs[FYD_MAX_SEGS / FYD_MAX_DISKS];
};

/* Context passed as SPDK app argument */
struct fyd_app_ctx {
    uint32_t            num_disks;
    struct fyd_disk_info disks[FYD_MAX_DISKS];
    char                addr[64];
    uint16_t            port;
};

/*
 * SPDK app start callback.
 * arg is a pointer to struct fyd_app_ctx.
 */
void fyd_startup(void *arg);

#ifdef __cplusplus
}
#endif
