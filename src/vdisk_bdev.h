#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

/*
 * Options for creating a fyd_vdisk bdev instance.
 * All string pointers must remain valid for the lifetime of the bdev.
 */
struct fyd_vdisk_opts {
    const char  *name;           /* bdev name, e.g. "disk0" */
    uint64_t     blocks;         /* total block count */
    uint32_t     block_size;     /* bytes per block (must be power-of-2 >= 512) */
    uint64_t     seg_blocks;     /* blocks per segment */
    /* Array of num_segs file paths for the segment backing files */
    uint32_t     num_segs;
    const char **seg_paths;      /* seg_paths[i] is the path for segment i */
};

/*
 * Register a new fyd_vdisk bdev with SPDK.
 * Must be called from an SPDK thread (app-thread or reactor).
 * Returns 0 on success, negative errno on failure.
 */
int fyd_vdisk_create(const struct fyd_vdisk_opts *opts);

/*
 * Unregister and free a fyd_vdisk bdev by name.
 * cb is called (with cb_arg) when unregistration completes.
 */
typedef void (*fyd_vdisk_destruct_cb)(void *cb_arg, int rc);
void fyd_vdisk_delete(const char *name, fyd_vdisk_destruct_cb cb, void *cb_arg);

#ifdef __cplusplus
}
#endif
