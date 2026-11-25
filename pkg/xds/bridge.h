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

#ifndef JFS_XDS_BRIDGE_H
#define JFS_XDS_BRIDGE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "proto.h"

#ifdef __cplusplus
extern "C" {
#endif

struct jfs_xds_rt_config {
    const char *bdev_name;
    const char *json_config_file;
    const char *reactor_mask;
    const char *rpc_addr;
    uint32_t main_core;
    uint32_t cluster_size_bytes;
    uint32_t metadata_page_count;
    const char *const *peer_bdfs;
    size_t peer_bdf_count;
    uint32_t device_backend;
    uint64_t blob_size_bytes;
    uint32_t blob_count;
    bool enable_fast_map;
};

struct jfs_xds_rt_info {
    uint64_t io_unit_size;
    uint64_t cluster_size;
    uint32_t worker_count;
    uint32_t direct_ready;
};

struct jfs_xds_bridge_iov {
    uintptr_t base;
    uint64_t len;
};

struct jfs_xds_bridge_desc {
    uintptr_t blob_handle;
    uintptr_t payload;
    uint64_t offset_bytes;
    uint64_t size_bytes;
    uint32_t iov_offset; /* index into flat iovs array */
    uint32_t iovcnt;
};

int jfs_xds_rt_start(const struct jfs_xds_rt_config *cfg,
    struct jfs_xds_rt_info *info_out);
int jfs_xds_rt_stop(void);
int jfs_xds_rt_lease_blobs(uint32_t count, uintptr_t *handles_out);
int jfs_xds_rt_release_blobs(const uintptr_t *handles, uint32_t count);
int jfs_xds_rt_import_buf(int p2p_fd, uint64_t device_va, uint64_t size,
    const char *const *peer_bdfs,
    const struct jfs_xds_peer_export *peers,
    size_t peer_count,
    uint64_t *reg_handle_out,
    uintptr_t *io_ptr_out);
int jfs_xds_rt_unregister_buf(uint64_t reg_handle);
int jfs_xds_rt_read_cpu(uintptr_t blob_handle, uint64_t blob_off,
    void *dst, uint64_t len);
int jfs_xds_rt_write_cpu(uintptr_t blob_handle, uint64_t blob_off,
    const void *src, uint64_t len);
int jfs_xds_rt_read_direct_batch(
    const struct jfs_xds_bridge_desc *descs, uint32_t desc_count,
    const struct jfs_xds_bridge_iov *iovs, uint32_t iov_count);

#ifdef __cplusplus
}
#endif

#endif /* JFS_XDS_BRIDGE_H */
