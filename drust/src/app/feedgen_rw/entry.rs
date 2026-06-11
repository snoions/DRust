#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(8))]
pub struct GlobalEntry {
    pub key: usize,
    pub value: [u8; 8],
}
 
