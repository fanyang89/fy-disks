/*
 * startup.c — SPDK startup callback for fy-disks
 *
 * Called by spdk_app_start() on the app thread after the SPDK framework
 * has been fully initialised.  We:
 *   1. Create an iSCSI portal group (PG 1, listening on addr:port)
 *   2. Create an iSCSI initiator group (IG 1, allow ANY from ANY)
 *   3. For each disk in fyd_app_ctx:
 *      a. Ensure segment backing files are pre-allocated
 *      b. Create the fyd_vdisk bdev
 *      c. Expose the bdev as an iSCSI target node (LUN 0)
 */

#include "startup.h"
#include "vdisk_bdev.h"

/* SPDK headers */
#include "spdk/event.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"

/* Internal iSCSI headers (in the SPDK source tree) */
#include "iscsi/tgt_node.h"
#include "iscsi/portal_grp.h"
#include "iscsi/init_grp.h"

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>

/* ── Utility: ensure a segment file exists and is pre-allocated ──────────── */

static int
ensure_seg_file(const char *path, uint64_t size_bytes)
{
    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        SPDK_ERRLOG("open(%s) failed: %s\n", path, strerror(errno));
        return -errno;
    }

    /* Check existing size */
    struct stat st;
    if (fstat(fd, &st) == 0 && (uint64_t)st.st_size >= size_bytes) {
        close(fd);
        return 0;
    }

    /* Pre-allocate space */
    int rc = posix_fallocate(fd, 0, (off_t)size_bytes);
    if (rc != 0) {
        /* Fallback: ftruncate extends (sparse) */
        if (ftruncate(fd, (off_t)size_bytes) != 0) {
            SPDK_ERRLOG("ftruncate(%s) failed: %s\n", path, strerror(errno));
            close(fd);
            return -errno;
        }
    }

    close(fd);
    return 0;
}

/* ── Startup callback ────────────────────────────────────────────────────── */

void
fyd_startup(void *arg)
{
    struct fyd_app_ctx *ctx = (struct fyd_app_ctx *)arg;
    int rc;

    /* ── 1. Portal group (PG 1) ─────────────────────────────────────────── */

    struct spdk_iscsi_portal_grp *pg = iscsi_portal_grp_create(1, false);
    if (!pg) {
        SPDK_ERRLOG("Failed to create portal group\n");
        spdk_app_stop(-1);
        return;
    }

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%u", (unsigned)ctx->port);

    struct spdk_iscsi_portal *portal = iscsi_portal_create(ctx->addr, port_str);
    if (!portal) {
        SPDK_ERRLOG("Failed to create portal %s:%s\n", ctx->addr, port_str);
        iscsi_portal_grp_destroy(pg);
        spdk_app_stop(-1);
        return;
    }

    iscsi_portal_grp_add_portal(pg, portal);

    rc = iscsi_portal_grp_register(pg);
    if (rc != 0) {
        SPDK_ERRLOG("Failed to register portal group: %d\n", rc);
        iscsi_portal_grp_destroy(pg);
        spdk_app_stop(-1);
        return;
    }

    rc = iscsi_portal_grp_open(pg, false);
    if (rc != 0) {
        SPDK_ERRLOG("Failed to open portal group: %d\n", rc);
        spdk_app_stop(-1);
        return;
    }

    /* ── 2. Initiator group (IG 1) — allow ANY from ANY ─────────────────── */

    char *inames[] = { (char *)"ANY" };
    char *imasks[] = { (char *)"ANY" };

    rc = iscsi_init_grp_create_from_initiator_list(
            1,                /* tag */
            1, inames,        /* 1 initiator name: ANY */
            1, imasks         /* 1 netmask: ANY */
    );
    if (rc != 0) {
        SPDK_ERRLOG("Failed to create initiator group: %d\n", rc);
        spdk_app_stop(-1);
        return;
    }

    /* ── 3. Per-disk: bdev + iSCSI target node ───────────────────────────── */

    for (uint32_t di = 0; di < ctx->num_disks; di++) {
        struct fyd_disk_info *d = &ctx->disks[di];

        uint64_t seg_size_bytes = d->seg_blocks * d->block_size;

        /* 3a. Ensure segment backing files exist */
        for (uint32_t si = 0; si < d->num_segs; si++) {
            rc = ensure_seg_file(d->segs[si].path, seg_size_bytes);
            if (rc != 0) {
                SPDK_ERRLOG("Failed to prepare segment file %s\n",
                            d->segs[si].path);
                spdk_app_stop(-1);
                return;
            }
        }

        /* 3b. Build fyd_vdisk_opts */
        const char **seg_paths = calloc(d->num_segs, sizeof(char *));
        if (!seg_paths) {
            spdk_app_stop(-1);
            return;
        }
        for (uint32_t si = 0; si < d->num_segs; si++) {
            seg_paths[si] = d->segs[si].path;
        }

        struct fyd_vdisk_opts opts = {
            .name       = d->name,
            .blocks     = d->blocks,
            .block_size = d->block_size,
            .seg_blocks = d->seg_blocks,
            .num_segs   = d->num_segs,
            .seg_paths  = seg_paths,
        };

        rc = fyd_vdisk_create(&opts);
        free(seg_paths);

        if (rc != 0) {
            SPDK_ERRLOG("fyd_vdisk_create(%s) failed: %d\n", d->name, rc);
            spdk_app_stop(-1);
            return;
        }

        /* 3c. iSCSI target node: map bdev to IQN */
        int pg_tags[1] = { 1 };
        int ig_tags[1] = { 1 };
        const char *bdev_names[1] = { d->name };
        int lun_ids[1] = { 0 };

        struct spdk_iscsi_tgt_node *tgt = iscsi_tgt_node_construct(
            -1,                 /* target_index: auto-assign */
            d->iqn,             /* name (IQN) */
            NULL,               /* alias */
            pg_tags, ig_tags, 1,/* pg/ig maps */
            bdev_names, lun_ids, 1, /* LUNs */
            128,                /* queue_depth */
            false,              /* disable_chap */
            false,              /* require_chap */
            false,              /* mutual_chap */
            0,                  /* chap_group */
            false,              /* header_digest */
            false               /* data_digest */
        );

        if (!tgt) {
            SPDK_ERRLOG("iscsi_tgt_node_construct(%s) failed\n", d->iqn);
            spdk_app_stop(-1);
            return;
        }

        SPDK_NOTICELOG("Registered iSCSI target %s -> bdev %s\n",
                       d->iqn, d->name);
    }

    SPDK_NOTICELOG("fy-disks: startup complete. "
                   "Listening on %s:%u\n", ctx->addr, (unsigned)ctx->port);
}
