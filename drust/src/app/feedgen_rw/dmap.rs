use crate::drust_std::{collections::dvec::{DVec, DVecRef}, sync::dmutex::DMutex};
use super::{entry::*, conf::*};

pub type Map = DVec<DMutex<GlobalEntry>>;
pub type MapRef<'a> = DVecRef<'a, DMutex<GlobalEntry>>;

pub struct KVStore;

impl KVStore {
    pub fn new() -> Map {
        let mut store = DVec::with_capacity(BUCKET_NUM);
        for _ in 0..BUCKET_NUM {
            store.push(DMutex::new(GlobalEntry { key: 0, value: [0u8; 8] }));
        }
        store
    }
}

pub async fn get(map: &MapRef<'_>, key: usize) -> [u8; 8] {
    let map_ref = map.as_ref();
    let bucket_id = bucket(key);
    let m = map_ref.get(bucket_id).unwrap();
    let value_ref = m.lock();
    let v = value_ref.value;
    m.unlock(value_ref);
    v
}

pub async fn put(map: &MapRef<'_>, key: usize, value: [u8; 8]) {
    let map_ref = map.as_ref();
    let bucket_id = bucket(key);
    let m = map_ref.get(bucket_id).unwrap();
    let mut value_ref = m.lock();
    value_ref.key = key;
    value_ref.value = value;
    m.unlock(value_ref);
}
