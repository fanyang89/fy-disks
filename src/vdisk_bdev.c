/*
 * fyd_vdisk bdev module
 *
 * A custom SPDK bdev that maps a virtual disk to multiple segment files,
 * using io_uring for I/O.  The implementation mirrors bdev_uring.c but
 * adds per-segment routing.
 *
 * I/O routing contract:
 *   bdev.optimal_io_boundary = seg_blocks
 *   bdev.split_on_optimal_io_boundary = 1
 * → SPDK guarantees that every submitted request is contained within a
 *   single segment, so submit_request never needs to split I/O itself.
 */

#include "vdisk_bdev.h"

#include "spdk/bdev_module.h"
#include "spdk/env.h"
#include "spdk/fd.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#include <liburing.h>

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#define FYD_URING_QUEUE_DEPTH  512
#define FYD_MAX_EVENTS_PER_POLL 32

/* ── Per-segment backend ────────────────────────────────────────────────── */

struct fyd_segment {
    int fd;        /* opened O_RDWR (with O_DIRECT if supported) */
};

/* ── Virtual-disk bdev structure ────────────────────────────────────────── */

struct fyd_vdisk {
    struct spdk_bdev    bdev;        /* MUST be first */
    struct fyd_segment *segs;
    uint64_t            seg_blocks;
    uint32_t            num_segs;
    char               *name;
};

/* ── Per-reactor I/O channel structs ────────────────────────────────────── */

/* All fyd_vdisk instances on the same reactor share one io_uring ring. */
struct fyd_group_channel {
    struct io_uring     ring;
    uint64_t            io_pending;
    uint64_t            io_inflight;
    struct spdk_poller *poller;
};

struct fyd_io_channel {
    struct fyd_group_channel *group_ch;
};

/* Stored in bdev_io->driver_ctx */
struct fyd_task {
    uint64_t              expected_len;
    struct fyd_io_channel *ch;
};

/* ── Module-level state ──────────────────────────────────────────────────── */

static struct spdk_bdev_module fyd_vdisk_module;  /* forward-declared */

/* ── Helper: get fyd_vdisk from bdev pointer ───────────────────────────── */

static inline struct fyd_vdisk *
vdisk_from_bdev(struct spdk_bdev *bdev)
{
    return SPDK_CONTAINEROF(bdev, struct fyd_vdisk, bdev);
}

/* ── Module callbacks ───────────────────────────────────────────────────── */

static int
fyd_vdisk_module_init(void)
{
    return 0;
}

static void
fyd_vdisk_module_fini(void)
{
    spdk_bdev_module_fini_done();
}

static int
fyd_vdisk_get_ctx_size(void)
{
    return sizeof(struct fyd_task);
}

static struct spdk_bdev_module fyd_vdisk_module = {
    .name        = "fyd_vdisk",
    .module_init = fyd_vdisk_module_init,
    .module_fini = fyd_vdisk_module_fini,
    .get_ctx_size = fyd_vdisk_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(fyd_vdisk, &fyd_vdisk_module)

/* ── Group channel poller: submit + reap ─────────────────────────────────── */

static int
fyd_group_poll(void *arg)
{
    struct fyd_group_channel *group_ch = arg;
    int to_submit = (int)group_ch->io_pending;
    int count = 0;
    int ret;

    if (to_submit > 0) {
        ret = io_uring_submit(&group_ch->ring);
        if (ret < 0) {
            SPDK_ERRLOG("io_uring_submit failed: %d\n", ret);
            return SPDK_POLLER_BUSY;
        }
        group_ch->io_pending  = 0;
        group_ch->io_inflight += (uint64_t)to_submit;
    }

    /* Reap completions */
    int to_complete = (int)group_ch->io_inflight;
    for (int i = 0; i < to_complete && i < FYD_MAX_EVENTS_PER_POLL; i++) {
        struct io_uring_cqe *cqe;
        ret = io_uring_peek_cqe(&group_ch->ring, &cqe);
        if (ret != 0) {
            break;
        }

        struct fyd_task      *task    = (struct fyd_task *)cqe->user_data;
        struct spdk_bdev_io  *bdev_io = spdk_bdev_io_from_ctx(task);
        enum spdk_bdev_io_status status;

        if (cqe->res == (int)task->expected_len) {
            status = SPDK_BDEV_IO_STATUS_SUCCESS;
        } else if (cqe->res == -EAGAIN || cqe->res == -EWOULDBLOCK) {
            status = SPDK_BDEV_IO_STATUS_NOMEM;
        } else {
            SPDK_ERRLOG("I/O failed: res=%d\n", cqe->res);
            status = SPDK_BDEV_IO_STATUS_FAILED;
        }

        group_ch->io_inflight--;
        io_uring_cqe_seen(&group_ch->ring, cqe);
        spdk_bdev_io_complete(bdev_io, status);
        count++;
    }

    return (count + to_submit > 0) ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

/* ── bdev fn_table: create / destroy IO channel ──────────────────────────── */

static int
fyd_bdev_create_cb(void *io_device, void *ctx_buf)
{
    struct fyd_io_channel *ch = ctx_buf;

    ch->group_ch = spdk_io_channel_get_ctx(
        spdk_get_io_channel(&fyd_vdisk_module));

    return 0;
}

static void
fyd_bdev_destroy_cb(void *io_device, void *ctx_buf)
{
    struct fyd_io_channel *ch = ctx_buf;

    spdk_put_io_channel(spdk_io_channel_from_ctx(ch->group_ch));
}

/* ── Module-level group channel (shared across all vdisks per reactor) ──── */

static int
fyd_group_create_cb(void *io_device, void *ctx_buf)
{
    struct fyd_group_channel *group_ch = ctx_buf;
    int rc;

    rc = io_uring_queue_init(FYD_URING_QUEUE_DEPTH, &group_ch->ring, 0);
    if (rc != 0) {
        SPDK_ERRLOG("io_uring_queue_init failed: %d\n", rc);
        return rc;
    }

    group_ch->io_pending  = 0;
    group_ch->io_inflight = 0;
    group_ch->poller = spdk_poller_register(fyd_group_poll, group_ch, 0);

    return 0;
}

static void
fyd_group_destroy_cb(void *io_device, void *ctx_buf)
{
    struct fyd_group_channel *group_ch = ctx_buf;

    spdk_poller_unregister(&group_ch->poller);
    io_uring_queue_exit(&group_ch->ring);
}

/* ── bdev fn_table: get_io_channel ──────────────────────────────────────── */

static struct spdk_io_channel *
fyd_bdev_get_io_channel(void *ctx)
{
    struct fyd_vdisk *vdisk = ctx;

    return spdk_get_io_channel(vdisk);
}

/* ── bdev fn_table: destruct ─────────────────────────────────────────────── */

static int
fyd_bdev_destruct(void *ctx)
{
    struct fyd_vdisk *vdisk = ctx;

    spdk_io_device_unregister(vdisk, NULL);

    for (uint32_t i = 0; i < vdisk->num_segs; i++) {
        if (vdisk->segs[i].fd >= 0) {
            close(vdisk->segs[i].fd);
        }
    }

    free(vdisk->segs);
    free(vdisk->name);
    free(vdisk->bdev.name);
    spdk_dma_free(vdisk);

    return 0;
}

/* ── bdev fn_table: io_type_supported ───────────────────────────────────── */

static bool
fyd_bdev_io_type_supported(void *ctx, enum spdk_bdev_io_type io_type)
{
    switch (io_type) {
    case SPDK_BDEV_IO_TYPE_READ:
    case SPDK_BDEV_IO_TYPE_WRITE:
    case SPDK_BDEV_IO_TYPE_FLUSH:
        return true;
    default:
        return false;
    }
}

/* ── I/O submission helper ───────────────────────────────────────────────── */

static void
fyd_get_buf_cb(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io, bool success)
{
    struct fyd_vdisk         *vdisk   = vdisk_from_bdev(bdev_io->bdev);
    struct fyd_io_channel    *fyd_ch  = spdk_io_channel_get_ctx(ch);
    struct fyd_group_channel *group   = fyd_ch->group_ch;
    struct fyd_task          *task    = (struct fyd_task *)bdev_io->driver_ctx;
    struct io_uring_sqe      *sqe;

    if (!success) {
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }

    uint64_t offset_blocks = bdev_io->u.bdev.offset_blocks;
    uint64_t num_blocks    = bdev_io->u.bdev.num_blocks;
    uint64_t seg_idx       = offset_blocks / vdisk->seg_blocks;
    uint64_t seg_off_bytes = (offset_blocks % vdisk->seg_blocks)
                             * vdisk->bdev.blocklen;
    uint64_t len_bytes     = num_blocks * vdisk->bdev.blocklen;

    sqe = io_uring_get_sqe(&group->ring);
    if (!sqe) {
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_NOMEM);
        return;
    }

    struct iovec *iovs  = bdev_io->u.bdev.iovs;
    int           iovcnt = bdev_io->u.bdev.iovcnt;

    if (bdev_io->type == SPDK_BDEV_IO_TYPE_READ) {
        io_uring_prep_readv(sqe, vdisk->segs[seg_idx].fd,
                            iovs, iovcnt, (off_t)seg_off_bytes);
    } else {
        io_uring_prep_writev(sqe, vdisk->segs[seg_idx].fd,
                             iovs, iovcnt, (off_t)seg_off_bytes);
    }

    io_uring_sqe_set_data(sqe, task);
    task->expected_len = len_bytes;
    task->ch           = fyd_ch;

    group->io_pending++;
}

/* ── bdev fn_table: submit_request ──────────────────────────────────────── */

static void
fyd_bdev_submit_request(struct spdk_io_channel *ch, struct spdk_bdev_io *bdev_io)
{
    switch (bdev_io->type) {
    case SPDK_BDEV_IO_TYPE_READ:
    case SPDK_BDEV_IO_TYPE_WRITE:
        spdk_bdev_io_get_buf(bdev_io, fyd_get_buf_cb,
            bdev_io->u.bdev.num_blocks * bdev_io->bdev->blocklen);
        break;
    case SPDK_BDEV_IO_TYPE_FLUSH:
        /* Phase-1: fsync each segment fd (synchronous; good enough for now) */
        {
            struct fyd_vdisk *vdisk = vdisk_from_bdev(bdev_io->bdev);
            for (uint32_t i = 0; i < vdisk->num_segs; i++) {
                fsync(vdisk->segs[i].fd);
            }
            spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_SUCCESS);
        }
        break;
    default:
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
        break;
    }
}

/* ── fn_table ────────────────────────────────────────────────────────────── */

static const struct spdk_bdev_fn_table fyd_vdisk_fn_table = {
    .destruct          = fyd_bdev_destruct,
    .submit_request    = fyd_bdev_submit_request,
    .io_type_supported = fyd_bdev_io_type_supported,
    .get_io_channel    = fyd_bdev_get_io_channel,
};

/* ── Public: fyd_vdisk_create ────────────────────────────────────────────── */

int
fyd_vdisk_create(const struct fyd_vdisk_opts *opts)
{
    if (!opts || !opts->name || opts->num_segs == 0 || !opts->seg_paths) {
        return -EINVAL;
    }

    struct fyd_vdisk *vdisk = spdk_dma_zmalloc(sizeof(*vdisk), 0, NULL);
    if (!vdisk) {
        return -ENOMEM;
    }

    vdisk->segs = calloc(opts->num_segs, sizeof(struct fyd_segment));
    if (!vdisk->segs) {
        spdk_dma_free(vdisk);
        return -ENOMEM;
    }

    vdisk->name      = strdup(opts->name);
    vdisk->seg_blocks = opts->seg_blocks;
    vdisk->num_segs   = opts->num_segs;

    /* Open segment files */
    for (uint32_t i = 0; i < opts->num_segs; i++) {
        int fd = open(opts->seg_paths[i], O_RDWR | O_DIRECT | O_CREAT, 0644);
        if (fd < 0) {
            /* Fall back without O_DIRECT (e.g. tmpfs / ext4 without directio) */
            fd = open(opts->seg_paths[i], O_RDWR | O_CREAT, 0644);
        }
        if (fd < 0) {
            SPDK_ERRLOG("Failed to open segment %u: %s\n",
                        i, opts->seg_paths[i]);
            for (uint32_t j = 0; j < i; j++) close(vdisk->segs[j].fd);
            free(vdisk->segs);
            free(vdisk->name);
            spdk_dma_free(vdisk);
            return -errno;
        }
        vdisk->segs[i].fd = fd;
    }

    /* Fill in SPDK bdev fields */
    vdisk->bdev.name        = strdup(opts->name);
    vdisk->bdev.product_name = "fyd Virtual Disk";
    vdisk->bdev.blocklen    = opts->block_size;
    vdisk->bdev.blockcnt    = opts->blocks;
    vdisk->bdev.fn_table    = &fyd_vdisk_fn_table;
    vdisk->bdev.module      = &fyd_vdisk_module;
    vdisk->bdev.ctxt        = vdisk;

    /* Critical: instruct SPDK to split I/O at segment boundaries */
    vdisk->bdev.optimal_io_boundary       = (uint32_t)opts->seg_blocks;
    vdisk->bdev.split_on_optimal_io_boundary = 1;

    /* Register group-level io_device (shared ring per reactor) */
    spdk_io_device_register(&fyd_vdisk_module,
                            fyd_group_create_cb, fyd_group_destroy_cb,
                            sizeof(struct fyd_group_channel),
                            "fyd_group");

    /* Register per-vdisk io_device */
    spdk_io_device_register(vdisk,
                            fyd_bdev_create_cb, fyd_bdev_destroy_cb,
                            sizeof(struct fyd_io_channel),
                            opts->name);

    int rc = spdk_bdev_register(&vdisk->bdev);
    if (rc != 0) {
        SPDK_ERRLOG("spdk_bdev_register failed for %s: %d\n", opts->name, rc);
        spdk_io_device_unregister(vdisk, NULL);
        for (uint32_t i = 0; i < opts->num_segs; i++) close(vdisk->segs[i].fd);
        free(vdisk->segs);
        free(vdisk->name);
        free(vdisk->bdev.name);
        spdk_dma_free(vdisk);
        return rc;
    }

    SPDK_NOTICELOG("fyd_vdisk: registered bdev '%s' %" PRIu64 " blocks x %u bytes, "
                   "%u segments of %" PRIu64 " blocks each\n",
                   opts->name, opts->blocks, opts->block_size,
                   opts->num_segs, opts->seg_blocks);
    return 0;
}

/* ── Public: fyd_vdisk_delete ────────────────────────────────────────────── */

struct fyd_delete_ctx {
    fyd_vdisk_destruct_cb cb;
    void                 *cb_arg;
};

static void
fyd_delete_bdev_cb(void *cb_arg, int rc)
{
    struct fyd_delete_ctx *ctx = cb_arg;
    if (ctx->cb) {
        ctx->cb(ctx->cb_arg, rc);
    }
    free(ctx);
}

void
fyd_vdisk_delete(const char *name, fyd_vdisk_destruct_cb cb, void *cb_arg)
{
    struct fyd_delete_ctx *ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        if (cb) cb(cb_arg, -ENOMEM);
        return;
    }
    ctx->cb     = cb;
    ctx->cb_arg = cb_arg;

    int rc = spdk_bdev_unregister_by_name(name, &fyd_vdisk_module,
                                          fyd_delete_bdev_cb, ctx);
    if (rc != 0) {
        if (cb) cb(cb_arg, rc);
        free(ctx);
    }
}
