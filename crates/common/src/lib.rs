#![forbid(unsafe_code)]

mod error;

pub use error::*;

pub const DEFAULT_PORT: u16 = 6399;
pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const MAX_CONNECTIONS: usize = 1024;
pub const INITIAL_BUFFER_CAPACITY: usize = 4 * 1024; // 4 KB
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024; // 64 MB
