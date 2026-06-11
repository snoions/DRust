use std::{fs::File, io::Write, sync::OnceLock};

use rand::{distributions::{Distribution, Uniform}, rngs::StdRng, SeedableRng, RngCore};
use tokio::task::JoinHandle;

use super::{dmap::{KVStore, MapRef, get, put}, entry::GlobalEntry, conf::{
    bucket, READ_RATIO, THREAD_NUM, UNIT_BUCKET_NUM, UNIT_THREAD_BUCKET_NUM,
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
// Must be inside the task because dspawn_to ships only the stack frame.
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

// Populate this partition via DMutex put (operates on the local copy).
pub async fn populate(map: MapRef<'_>, server: usize, thread: usize) {
    let keys = load_partition_keys(server, thread);
    for key in keys {
        put(&map, key, VALUE).await;
    }
}

pub async fn benchmark(map: MapRef<'_>, server: usize, thread: usize, num_entries: usize) -> usize {
    let keys = load_partition_keys(server, thread);
    // Build the graph lazily on the first benchmark task per server, before timing.
    let graph = GRAPH.get_or_init(|| build_graph(num_entries));
    let start = tokio::time::Instant::now();
    let cnt = keys.len();
    let mut rng = StdRng::seed_from_u64(1);
    let rand_dist = Uniform::from(0u32..u32::MAX);
    let mut wr_rng = StdRng::seed_from_u64(2);
    let wr_dist = Uniform::from(0..10i32);

    for (op_idx, &key) in keys.iter().enumerate() {
        let r = wr_dist.sample(&mut wr_rng);
        if r < READ_RATIO {
            let getv = get(&map, key).await;
            if getv != VALUE {
                panic!("Wrong value");
            }
        } else {
            put(&map, key, VALUE).await;
        }
        if op_idx % TRAVERSAL_INTERVAL == 0 {
            traverse(graph, rand_dist.sample(&mut rng));
        }
    }

    let duration = start.elapsed();
    println!(
        "Thread Local Elapsed Time: {:?}, throughput: {:?} Mops/s",
        duration,
        (cnt as f64 / duration.as_secs_f64()) / 1000000.0
    );
    duration.as_nanos() as usize
}

pub async fn zipf_bench() {
    let map = KVStore::new();
    let num_entries = NUM_ARRAY_ENTRIES / NUM_SERVERS;

    // Count total ops for aggregate throughput. Keys load inside each task.
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
    let popstart = tokio::time::Instant::now();
    let mut handles: Vec<JoinHandle<()>> = vec![];
    for server in 0..NUM_SERVERS {
        for thread in 0..THREAD_NUM {
            let map_ref = map.as_dref();
            let handle = dspawn_to(
                populate(map_ref, server, thread),
                GLOBAL_HEAP_START + server * WORKER_UNIT_SIZE,
            );
            handles.push(handle);
        }
    }
    for handle in handles {
        handle.await;
    }
    println!("Populate Elapsed Time: {:?} seconds", popstart.elapsed());

    // ── Benchmark phase ───────────────────────────────────────────────────────
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
        thread_times.push(nanos as f64 / 1e9);
    }

    let total_wall = start.elapsed();
    let avg_time = thread_times.iter().sum::<f64>() / thread_times.len() as f64;
    // Aggregate throughput: all ops across all threads over avg_time
    let aggregate_throughput = (op_total as f64 / avg_time) / 1000000.0;
    println!(
        "Average Thread Elapsed Time: {:?} seconds",
        avg_time
    );
    println!(
        "Total Wall Time: {:?} seconds, aggregate throughput: {:?} Mops/s",
        total_wall, aggregate_throughput
    );

    let file_name = format!(
        "{}/DRust_home/logs/feedgenrw_drust_{}.txt",
        dirs::home_dir().unwrap().display(),
        NUM_SERVERS
    );
    let mut wrt_file = File::create(file_name).expect("file");
    writeln!(wrt_file, "Average Thread Elapsed Time: {} seconds", avg_time).expect("write");
    writeln!(wrt_file, "Total Wall Time: {} seconds", total_wall.as_secs_f64()).expect("write");
    writeln!(wrt_file, "Aggregate Throughput: {} Mops/s", aggregate_throughput).expect("write");
}

