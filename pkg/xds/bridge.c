//go:build linux && xds

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "bridge.h"

#include <errno.h>
#include <semaphore.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>

#include "xds/blob_pool.h"
#include "xds/dma_registry.h"
#include "xds/io.h"
#include "xds/runtime.h"

#define JFS_XDS_STACK_DESCS 32u
#define JFS_XDS_STACK_IOVS  64u

struct sync_io_waiter {
    sem_t sem;
    int status;
};

static void sync_io_cb(void *cb_arg, int status,
    enum xds_completion_result_type result_type, uint64_t result)
{
    struct sync_io_waiter *w = (struct sync_io_waiter *)cb_arg;
    (void)result_type;
    (void)result;
    w->status = status;
    sem_post(&w->sem);
}

static int wait_sem(sem_t *sem)
{
    int rc;
    do {
        rc = sem_wait(sem);
    } while (rc != 0 && errno == EINTR);
    return rc == 0 ? 0 : -errno;
}

static uint64_t align_up_u64(uint64_t v, uint64_t align)
{
    return (v + align - 1) & ~(align - 1);
}

int jfs_xds_rt_start(const struct jfs_xds_rt_config *cfg,
    struct jfs_xds_rt_info *info_out)
{
    struct xds_runtime_config rt_cfg = {0};
    struct xds_blob_pool_config pool_cfg = {0};
    int rc;

    if (!cfg || !info_out)
        return -EINVAL;

    rt_cfg.bdev_name = cfg->bdev_name;
    rt_cfg.json_config_file = cfg->json_config_file;
    rt_cfg.reactor_mask = cfg->reactor_mask;
    rt_cfg.sock = cfg->rpc_addr;
    rt_cfg.main_core = cfg->main_core;
    rt_cfg.cluster_size_bytes = cfg->cluster_size_bytes;
    rt_cfg.metadata_page_count = cfg->metadata_page_count;
    rt_cfg.peer_bdfs = cfg->peer_bdfs;
    rt_cfg.peer_bdf_count = cfg->peer_bdf_count;
    rt_cfg.device_backend = (enum xds_device_backend)cfg->device_backend;

    rc = xds_runtime_start(&rt_cfg);
    if (rc != 0)
        return rc;

    pool_cfg.blob_size_bytes = cfg->blob_size_bytes;
    pool_cfg.blob_count = cfg->blob_count;
    pool_cfg.queue_count = 1;
    pool_cfg.enable_fast_map = cfg->enable_fast_map;

    rc = xds_blob_pool_set_config(&pool_cfg);
    if (rc == 0)
        rc = xds_blob_pool_init();
    if (rc != 0) {
        (void)xds_runtime_stop();
        return rc;
    }

    info_out->io_unit_size = xds_runtime_get_io_unit_size();
    info_out->cluster_size = xds_runtime_get_cluster_size();
    info_out->worker_count = xds_runtime_get_worker_count();
    info_out->direct_ready = xds_direct_is_enabled() ? 1u : 0u;
    return 0;
}

int jfs_xds_rt_stop(void)
{
    int rc1 = xds_blob_pool_teardown();
    int rc2 = xds_runtime_stop();
    return rc1 != 0 ? rc1 : rc2;
}

int jfs_xds_rt_lease_blobs(uint32_t count, uintptr_t *handles_out)
{
    if (!handles_out || count == 0)
        return -EINVAL;

    for (uint32_t i = 0; i < count; i++) {
        xds_blob_handle_t h = NULL;
        int rc = xds_blob_pool_get_blob(0, 5000, &h);
        if (rc != 0) {
            for (uint32_t j = 0; j < i; j++) {
                (void)xds_blob_pool_release_blob(
                    0, (xds_blob_handle_t)handles_out[j]);
            }
            return rc;
        }
        handles_out[i] = (uintptr_t)h;
    }
    return 0;
}

int jfs_xds_rt_release_blobs(const uintptr_t *handles, uint32_t count)
{
    int first_rc = 0;
    if (!handles)
        return 0;

    for (uint32_t i = 0; i < count; i++) {
        if (!handles[i])
            continue;
        int rc = xds_blob_pool_release_blob(
            0, (xds_blob_handle_t)handles[i]);
        if (rc != 0 && first_rc == 0)
            first_rc = rc;
    }
    return first_rc;
}

int jfs_xds_rt_import_buf(int p2p_fd, uint64_t device_va, uint64_t size,
    const char *const *peer_bdfs,
    const struct jfs_xds_peer_export *peers,
    size_t peer_count,
    uint64_t *reg_handle_out,
    uintptr_t *io_ptr_out)
{
    struct xds_dma_import_peer xds_peers[XDS_CTRL_MAX_PEERS];
    uint64_t reg;
    void *io_ptr;

    if (peer_count == 0 || peer_count > XDS_CTRL_MAX_PEERS ||
        !peer_bdfs || !peers || !reg_handle_out || !io_ptr_out) {
        if (p2p_fd >= 0)
            close(p2p_fd);
        return -EINVAL;
    }

    memset(xds_peers, 0, sizeof(xds_peers));
    for (size_t i = 0; i < peer_count; i++) {
        strncpy(xds_peers[i].peer_bdf, peer_bdfs[i],
            sizeof(xds_peers[i].peer_bdf) - 1);
        xds_peers[i].exported.handle = peers[i].handle;
        xds_peers[i].exported.backend = peers[i].backend;
        xds_peers[i].exported.page_size = peers[i].page_size;
        xds_peers[i].exported.mapped_len = peers[i].mapped_len;
        xds_peers[i].exported.segment_count = peers[i].segment_count;
        xds_peers[i].exported.first_dma_addr = peers[i].first_dma_addr;
    }

    /* xds_direct_import_buffer consumes p2p_fd on both success and failure. */
    reg = xds_direct_import_buffer(p2p_fd, (void *)(uintptr_t)device_va,
        size, xds_peers, peer_count);
    if (reg == 0)
        return -ENODEV;

    io_ptr = xds_direct_buffer_io_ptr(reg);
    if (!io_ptr) {
        (void)xds_direct_unregister_buffer(reg);
        return -ENODEV;
    }

    *reg_handle_out = reg;
    *io_ptr_out = (uintptr_t)io_ptr;
    return 0;
}

int jfs_xds_rt_unregister_buf(uint64_t reg_handle)
{
    return xds_direct_unregister_buffer(reg_handle);
}

int jfs_xds_rt_read_cpu(uintptr_t blob_handle, uint64_t blob_off,
    void *dst, uint64_t len)
{
    uint64_t unit = xds_runtime_get_io_unit_size();
    uint64_t aligned_len;
    void *dma_buf;
    struct xds_io_desc desc = {0};
    struct sync_io_waiter w = {0};
    int rc;

    if (!blob_handle || !dst || len == 0 || unit == 0 || (blob_off % unit) != 0)
        return -EINVAL;

    aligned_len = align_up_u64(len, unit);
    dma_buf = xds_alloc_io_buffer(aligned_len);
    if (!dma_buf)
        return -ENOMEM;

    if (sem_init(&w.sem, 0, 0) != 0) {
        xds_free_io_buffer(dma_buf);
        return -errno;
    }

    desc.handle = (void *)blob_handle;
    desc.payload = dma_buf;
    desc.offset_bytes = blob_off;
    desc.size_bytes = aligned_len;

    rc = xds_submit_read(&desc, sync_io_cb, &w);
    if (rc == 0)
        rc = wait_sem(&w.sem);
    if (rc == 0)
        rc = w.status;
    if (rc == 0)
        memcpy(dst, dma_buf, (size_t)len);

    sem_destroy(&w.sem);
    xds_free_io_buffer(dma_buf);
    return rc;
}

int jfs_xds_rt_write_cpu(uintptr_t blob_handle, uint64_t blob_off,
    const void *src, uint64_t len)
{
    uint64_t unit = xds_runtime_get_io_unit_size();
    uint64_t aligned_len;
    void *dma_buf;
    struct xds_io_desc desc = {0};
    struct sync_io_waiter w = {0};
    int rc;

    if (!blob_handle || !src || len == 0 || unit == 0 || (blob_off % unit) != 0)
        return -EINVAL;

    aligned_len = align_up_u64(len, unit);
    dma_buf = xds_alloc_io_buffer(aligned_len);
    if (!dma_buf)
        return -ENOMEM;

    memcpy(dma_buf, src, (size_t)len);
    if (aligned_len > len)
        memset((char *)dma_buf + len, 0, (size_t)(aligned_len - len));

    if (sem_init(&w.sem, 0, 0) != 0) {
        xds_free_io_buffer(dma_buf);
        return -errno;
    }

    desc.handle = (void *)blob_handle;
    desc.payload = dma_buf;
    desc.offset_bytes = blob_off;
    desc.size_bytes = aligned_len;

    rc = xds_submit_write(&desc, sync_io_cb, &w);
    if (rc == 0)
        rc = wait_sem(&w.sem);
    if (rc == 0)
        rc = w.status;

    sem_destroy(&w.sem);
    xds_free_io_buffer(dma_buf);
    return rc;
}

int jfs_xds_rt_read_direct_batch(
    const struct jfs_xds_bridge_desc *descs, uint32_t desc_count,
    const struct jfs_xds_bridge_iov *iovs, uint32_t iov_count)
{
    struct xds_io_desc stack_descs[JFS_XDS_STACK_DESCS];
    struct iovec stack_iovs[JFS_XDS_STACK_IOVS];
    struct xds_io_desc *xds_descs = stack_descs;
    struct iovec *xds_iovs = stack_iovs;
    struct sync_io_waiter w = {0};
    size_t total_iovs = 0;
    size_t iov_cursor = 0;
    int rc;

    if (!descs || desc_count == 0)
        return -EINVAL;

    for (uint32_t i = 0; i < desc_count; i++) {
        if (descs[i].iovcnt > 0) {
            if (!iovs ||
                descs[i].iov_offset > iov_count ||
                descs[i].iovcnt > iov_count - descs[i].iov_offset) {
                return -EINVAL;
            }
            total_iovs += descs[i].iovcnt;
        }
    }

    if (desc_count > JFS_XDS_STACK_DESCS) {
        xds_descs = calloc(desc_count, sizeof(*xds_descs));
        if (!xds_descs)
            return -ENOMEM;
    } else {
        memset(stack_descs, 0, (size_t)desc_count * sizeof(*xds_descs));
    }

    if (total_iovs > JFS_XDS_STACK_IOVS) {
        xds_iovs = calloc(total_iovs, sizeof(*xds_iovs));
        if (!xds_iovs) {
            if (xds_descs != stack_descs)
                free(xds_descs);
            return -ENOMEM;
        }
    }

    for (uint32_t i = 0; i < desc_count; i++) {
        xds_descs[i].handle = (void *)descs[i].blob_handle;
        xds_descs[i].payload = (void *)descs[i].payload;
        xds_descs[i].offset_bytes = descs[i].offset_bytes;
        xds_descs[i].size_bytes = descs[i].size_bytes;
        if (descs[i].iovcnt > 0) {
            struct iovec *dst_iov = &xds_iovs[iov_cursor];
            const struct jfs_xds_bridge_iov *src_iov =
                &iovs[descs[i].iov_offset];
            for (uint32_t j = 0; j < descs[i].iovcnt; j++) {
                dst_iov[j].iov_base = (void *)src_iov[j].base;
                dst_iov[j].iov_len = (size_t)src_iov[j].len;
            }
            xds_descs[i].iovs = dst_iov;
            xds_descs[i].iovcnt = descs[i].iovcnt;
            iov_cursor += descs[i].iovcnt;
        }
    }

    if (sem_init(&w.sem, 0, 0) != 0) {
        rc = -errno;
        goto out;
    }

    rc = xds_submit_read_batch(xds_descs, desc_count, sync_io_cb, &w);
    if (rc == 0)
        rc = wait_sem(&w.sem);
    if (rc == 0)
        rc = w.status;
    sem_destroy(&w.sem);

out:
    if (xds_iovs != stack_iovs)
        free(xds_iovs);
    if (xds_descs != stack_descs)
        free(xds_descs);
    return rc;
}
