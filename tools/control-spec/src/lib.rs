use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemSpec {
    pub n: usize,
    pub f: usize,
    // address list is long and fixed, send a dedicated file ahead of time for it
    // pub addrs: Vec<SocketAddr>,
    pub num_block_packet: usize,
    pub block_size: usize,
    pub chunk_size: usize,
    // k is derived from block_size and chunk_size
    pub degree: usize,

    pub protocol: Protocol,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Protocol {
    Entropy,
    Glacier,
    Replication,
}

impl SystemSpec {
    pub fn k(&self) -> usize {
        self.block_size / self.chunk_size
    }

    pub fn num_correct_packet(&self) -> usize {
        (self.n - 2 * self.f) * self.num_block_packet
    }

    pub fn csv_row(&self) -> String {
        format!(
            "{},{},{:?},{},{},{},{},{},{}",
            self.n,
            self.f,
            self.protocol,
            self.block_size,
            self.chunk_size,
            self.num_block_packet,
            self.k(),
            self.num_correct_packet(),
            self.degree
        )
    }
}
