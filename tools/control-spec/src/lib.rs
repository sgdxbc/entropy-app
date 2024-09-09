use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemSpec {
    pub n: usize,
    pub f: usize,
    // address list is long and fixed, send a dedicated file ahead of time for it
    // pub addrs: Vec<SocketAddr>,
    pub num_block_packet: usize,
    pub chunk_size: usize,
    // k is derived from num_block_packet and f
}
