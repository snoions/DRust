use crate::drust_std::collections::dvec::{DVec, DVecRef};
use crate::conf::{GLOBAL_HEAP_START, LOCAL_HEAP_START};
use crate::drust_std::comm::drust_write_sync;
use crate::drust_std::primitives::{current_place, Destination, drust_write_large_sync};
use crate::drust_std::alloc::LOCAL_ALLOCATOR;
use std::{mem, thread};

use super::{entry::*, conf::*};

pub type Map = DVec<GlobalEntry>;
pub type MapRef<'a> = DVecRef<'a, GlobalEntry>;

pub struct KVStore;

impl KVStore {
    pub fn new() -> Map {
        let mut store = DVec::with_capacity(BUCKET_NUM);
        for _ in 0..BUCKET_NUM {
            store.push(GlobalEntry { key: 0, value: [0u8; 8] });
        }
        store
    }
}

// Read via DVecRef — the first access triggers local_copy() which fetches the
// whole vec; subsequent accesses (and accesses from other DVecRef instances
// sharing the same orig_raw.0 via REF_MAP) are local. The warmup phase primes
// REF_MAP so the timed benchmark reads locally.
pub async fn get(map: &MapRef<'_>, key: usize) -> [u8; 8] {
    let map_ref = map.as_ref();
    let bucket_id = bucket(key);
    map_ref.get(bucket_id).unwrap().value
}

// Direct per-element write — used only during populate before concurrent
// reads begin, so no lock is needed.
pub async fn put(map: &MapRef<'_>, key: usize, value: [u8; 8]) {
    let bucket_id = bucket(key);
    let entry_addr = map.orig_raw.0 + bucket_id * mem::size_of::<GlobalEntry>();
    if current_place(entry_addr) == Destination::Local {
        unsafe {
            let e = &mut *(entry_addr as *mut GlobalEntry);
            e.key = key;
            e.value = value;
        }
    } else {
        let mut entry = GlobalEntry { key, value };
        unsafe {
            drust_write_sync(
                &mut entry as *mut GlobalEntry as usize - LOCAL_HEAP_START,
                entry_addr - GLOBAL_HEAP_START,
                mem::size_of::<GlobalEntry>(),
                thread::current().id().as_u64().get() as usize,
            );
        }
    }
}

// Bulk-populate a contiguous range of buckets [start_bucket, end_bucket) by
// building the entire range in a local buffer and writing it with one
// drust_write_large_sync (chunked into <=1GB RDMA transfers) instead of one
// synchronous RDMA write per key. `keys` lists the keys to set to VALUE within
// this range; all other buckets in the range are left zero-initialized.
//
// This is correct because the writing server is the one that later reads the
// range (per-server populate dispatch), and drust_write_large_sync is coherent
// with the drust_read_large_sync used by reads.
pub async fn populate_range(
    map: &MapRef<'_>,
    start_bucket: usize,
    end_bucket: usize,
    keys: &[usize],
    value: [u8; 8],
) {
    let n = end_bucket - start_bucket;

    // Bulk-write the whole range to the canonical map.
    let dst_addr = map.orig_raw.0 + start_bucket * mem::size_of::<GlobalEntry>();
    if current_place(dst_addr) == Destination::Local {
        // Local home: write directly into the canonical map, no staging buffer.
        // Zero the range first, then set populated buckets. Avoids a second
        // multi-GB allocation on the home server.
        unsafe {
            let base = dst_addr as *mut GlobalEntry;
            for i in 0..n {
                *base.add(i) = GlobalEntry { key: 0, value: [0u8; 8] };
            }
            for &key in keys {
                let b = bucket(key);
                if b >= start_bucket && b < end_bucket {
                    *base.add(b - start_bucket) = GlobalEntry { key, value };
                }
            }
        }
        return;
    }

    // Remote home: stage the range in a local buffer, then one bulk RDMA write.
    let mut buf: Vec<GlobalEntry, &good_memory_allocator::SpinLockedAllocator> =
        unsafe { Vec::with_capacity_in(n, &LOCAL_ALLOCATOR) };
    for _ in 0..n {
        buf.push(GlobalEntry { key: 0, value: [0u8; 8] });
    }
    for &key in keys {
        let b = bucket(key);
        if b >= start_bucket && b < end_bucket {
            buf[b - start_bucket] = GlobalEntry { key, value };
        }
    }
    let src_addr = buf.as_ptr() as usize;
    {
        unsafe {
            drust_write_large_sync(
                src_addr - LOCAL_HEAP_START,
                dst_addr - GLOBAL_HEAP_START,
                n * mem::size_of::<GlobalEntry>(),
                thread::current().id().as_u64().get() as usize,
            );
        }
    }
}
