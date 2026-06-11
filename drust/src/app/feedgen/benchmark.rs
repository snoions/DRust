use std::{fs::File, io::Write, sync::OnceLock};

use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng, RngCore};
use tokio::task::JoinHandle;

use super::{dmap::{KVStore, MapRef, get, populate_range}, entry::GlobalEntry, conf::{
    bucket, THREAD_NUM, UNIT_BUCKET_NUM, UNIT_THREAD_BUCKET_NUM, BUCKET_NUM,
}};

use crate::{
    conf::{GLOBAL_HEAP_START, NUM_SERVERS, WORKER_UNIT_SIZE},
    drust_std::{collections::dvec::DVecRef, thread::dspawn_to},
};



const VALUE: [u8; 8] = [b'x'; 8];

const ARRAY_ENTRY_SIZE: usize = 8192;
const NUM_ARRAY_ENTRIES: usize = 2 << 20;
const GRAPH_STRIDE: usize = 64;
const GRAPH_HOPS: usize = ARRAY_ENTRY_SIZE / GRAPH_STRIDE;
const TRAVERSAL_INTERVAL: usize = 32;
const NUM_ITERATIONS: usize = 10;

type GraphNode = [u8; ARRAY_ENTRY_SIZE];

static GRAPH: OnceLock<Vec<GraphNode>> = OnceLock::new();

fn build_graph(num_entries: usize) -> Vec<GraphNode> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut indices: Vec<u8> = (0..GRAPH_HOPS as u8).collect();
    for i in (1..GRAPH_HOPS).rev() {
        let j = (rng.next_u32() as usize) % (i + 1);
        indices.swap(i, j);
    }
    let mut sample: GraphNode = [0u8; ARRAY_ENTRY_SIZE];
    for i in 0..GRAPH_HOPS {
        sample[indices[i] as usize * GRAPH_STRIDE] = indices[(i + 1) % GRAPH_HOPS];
    }
    vec![sample; num_entries]
}

#[inline]
fn traverse(graph: &[GraphNode], rand_val: u32) {
    let entry = &graph[rand_val as usize % graph.len()];
    let mut current: usize = 0;
    let mut visited = [0u8; GRAPH_HOPS];
    for _ in 0..GRAPH_HOPS {
        visited[current] = 1;
        current = entry[current * GRAPH_STRIDE] as usize;
    }
    std::hint::black_box(visited);
}

// Load this (server, thread) partition's keys by reading the CSV locally.
// Must be done inside the task because dspawn_to ships only the stack frame —
// a Vec allocated on the driver would have a dangling pointer on a remote server.
fn load_partition_keys(server: usize, thread: usize) -> Vec<usize> {
    let csv_file = format!(
        "{}/DRust_home/dataset/zipf.csv",
        dirs::home_dir().unwrap().display()
    );
    let mut keys = vec![];
    let mut rdr = csv::Reader::from_path(&csv_file).unwrap();
    for result in rdr.records() {
        let record = result.unwrap();
        let key: usize = record[0].parse().unwrap();
        let bkt = bucket(key);
        let s = bkt / UNIT_BUCKET_NUM;
        let t = (bkt % UNIT_BUCKET_NUM) / UNIT_THREAD_BUCKET_NUM;
        if s == server && t == thread {
            keys.push(key);
        }
    }
    keys
}

// Populate the ENTIRE map on server 0 with a single local bulk write.
// The map's canonical storage lives on server 0, so writing it here is a local
// CPU copy (no RDMA) and produces a consistent map that every server then reads
// via local_copy. Reads all return VALUE because every populated bucket is set.
// Runs on server 0 only (dispatched to GLOBAL_HEAP_START).
pub async fn populate_all(map: MapRef<'_>) {
    // Read every key from the CSV and set its bucket to VALUE.
    let csv_file = format!(
        "{}/DRust_home/dataset/zipf.csv",
        dirs::home_dir().unwrap().display()
    );
    let mut keys = vec![];
    let mut rdr = csv::Reader::from_path(&csv_file).unwrap();
    for result in rdr.records() {
        let record = result.unwrap();
        let key: usize = record[0].parse().unwrap();
        keys.push(key);
    }
    // One bulk write covering the whole bucket space [0, BUCKET_NUM).
    populate_range(&map, 0, BUCKET_NUM, &keys, VALUE).await;
}


// Readonly benchmark — all operations are gets, no read/write sampling needed.
// Returns this thread's elapsed time in nanoseconds.
pub async fn benchmark(map: MapRef<'_>, server: usize, thread: usize, num_entries: usize) -> usize {
    let keys = load_partition_keys(server, thread);
    // Build the graph lazily on first benchmark task per server, before timing.
    let graph = GRAPH.get_or_init(|| build_graph(num_entries));
    let start = tokio::time::Instant::now();
    let cnt = keys.len();
    let mut rng = StdRng::seed_from_u64(1);
    let rand_dist = Uniform::from(0u32..u32::MAX);

    for _ in (0..NUM_ITERATIONS) {
        for (op_idx, key) in keys.iter().enumerate() {
            let getv = get(&map, *key).await;
            if getv != VALUE {
                let b = bucket(*key);
                let start_bucket = server * UNIT_BUCKET_NUM + thread * UNIT_THREAD_BUCKET_NUM;
                let end_bucket = (start_bucket + UNIT_THREAD_BUCKET_NUM).min(BUCKET_NUM);
                let in_range = b >= start_bucket && b < end_bucket;
                panic!(
                    "Wrong value: server_idx={} server_arg={} thread={} key={} bucket={} \
                     populate_range=[{},{}) in_range={} got={:?} expected={:?}",
                    unsafe { crate::conf::SERVER_INDEX },
                    server, thread, key, b, start_bucket, end_bucket, in_range, getv, VALUE
                );
            }
            if op_idx % TRAVERSAL_INTERVAL == 0 {
                traverse(graph, rand_dist.sample(&mut rng));
            }
        }
    }

    let duration = start.elapsed();
    println!(
        "Thread Local Elapsed Time: {:?}, throughput: {:?} Mops/s",
        duration,
        ((cnt as f64 * NUM_ITERATIONS as f64) / duration.as_secs_f64()) / 1000000.0
    );
    duration.as_nanos() as usize
}

pub async fn zipf_bench() {
    let map = KVStore::new();
    let num_entries = NUM_ARRAY_ENTRIES / NUM_SERVERS;

    // Count total ops for aggregate throughput. Keys themselves are loaded
    // once per server (server_partitions) inside the tasks, because dspawn_to
    // ships only the stack frame — a Vec built here would dangle on remotes.
    let csv_file = format!(
        "{}/DRust_home/dataset/zipf.csv",
        dirs::home_dir().unwrap().display()
    );
    let mut op_total: u64 = 0;
    {
        let mut rdr = csv::Reader::from_path(&csv_file).unwrap();
        for result in rdr.records() {
            let _ = result.unwrap();
            op_total += 1;
        }
    }
    println!("op_total={}", op_total);

    // ── Populate phase ────────────────────────────────────────────────────────
    // Server 0 populates the entire map locally in one task. The map lives on
    // server 0, so this is a local write with no cross-server RDMA write/read
    // visibility gap. Every server then reads the consistent map via local_copy.
    let popstart = tokio::time::Instant::now();
    let map_ref = map.as_dref();
    let handle: JoinHandle<()> = dspawn_to(populate_all(map_ref), GLOBAL_HEAP_START);
    handle.await;
    println!("Populate Elapsed Time: {:?} seconds", popstart.elapsed());

    // ── Benchmark phase ───────────────────────────────────────────────────────
    // Graph is built lazily inside the first benchmark task per server.
    let start = tokio::time::Instant::now();
    let mut handles: Vec<JoinHandle<usize>> = vec![];
    for server in 0..NUM_SERVERS {
        for thread in 0..THREAD_NUM {
            let map_ref = map.as_dref();
            let handle = dspawn_to(
                benchmark(map_ref, server, thread, num_entries),
                GLOBAL_HEAP_START + server * WORKER_UNIT_SIZE,
            );
            handles.push(handle);
        }
    }
    let mut thread_times: Vec<f64> = vec![];
    for handle in handles {
        let nanos = handle.await.unwrap();
        thread_times.push(nanos as f64 / 1e9);   // ns -> seconds
    }

    let total_wall = start.elapsed();
    let avg_time = thread_times.iter().sum::<f64>() / thread_times.len() as f64;
    // Aggregate throughput: all ops across all threads over the wall-clock time.
    let aggregate_throughput = ((op_total as f64 * NUM_ITERATIONS as f64) / avg_time) / 1000000.0;
    println!(
        "Average Thread Elapsed Time: {:?} seconds",
        avg_time
    );
    println!(
        "Total Wall Time: {:?} seconds, aggregate throughput: {:?} Mops/s",
        total_wall, aggregate_throughput
    );

    let file_name = format!(
        "{}/DRust_home/logs/feedgen_drust_{}.txt",
        dirs::home_dir().unwrap().display(),
        NUM_SERVERS
    );
    let mut wrt_file = File::create(file_name).expect("file");
    writeln!(wrt_file, "Average Thread Elapsed Time: {} seconds", avg_time).expect("write");
    writeln!(wrt_file, "Total Wall Time: {} seconds", total_wall.as_secs_f64()).expect("write");
    writeln!(wrt_file, "Aggregate Throughput: {} Mops/s", aggregate_throughput).expect("write");
}
