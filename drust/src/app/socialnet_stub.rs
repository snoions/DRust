use crate::drust_std::collections::dvec::DVec;

#[derive(Default, Clone)]
pub struct Image { pub width: u32, pub height: u32, pub pixels: DVec<u8> }

pub mod media {
    pub use super::Image;
}
