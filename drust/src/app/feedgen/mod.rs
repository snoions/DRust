use crate::{conf::NUM_SERVERS, drust_std::utils::{ResourceManager, COMPUTES}};
use self::conf::THREAD_NUM;

pub mod entry;
pub mod conf;
pub mod benchmark;
pub mod dmap;


// load column from file and return a Column struct
pub async fn run() {
    unsafe{
        COMPUTES = Some(ResourceManager::new(NUM_SERVERS * THREAD_NUM));
    }
    benchmark::zipf_bench().await;
}
