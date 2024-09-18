use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemSpec {
    // network
    pub n: usize,
    pub f: usize,
    pub node_bandwidth: usize,
    // address list is long and fixed, send a dedicated file ahead of time for it
    // pub addrs: Vec<SocketAddr>,
    pub protocol: Protocol,

    // entropy & glacier
    pub chunk_size: usize,
    pub k: usize,
    // block_size is derived from chunk_size and k

    // entropy
    pub num_block_packet: usize,
    pub degree: usize,

    // glacier
    pub group_size: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Protocol {
    Entropy,
    Glacier,
    Replication,
}

impl SystemSpec {
    pub fn block_size(&self) -> usize {
        self.chunk_size * self.k
    }

    pub fn num_correct_packet(&self) -> usize {
        match self.protocol {
            Protocol::Entropy => (self.n - 2 * self.f) * self.num_block_packet,
            Protocol::Glacier => self.group_size - 2 * self.f,
            Protocol::Replication => self.n - self.f,
        }
    }

    pub fn csv_row(&self) -> String {
        format!(
            "{},{},{:?},{},{},{},{},{},{}",
            self.n,
            self.f,
            self.protocol,
            self.chunk_size,
            self.k,
            self.num_block_packet,
            self.num_correct_packet(),
            self.degree,
            self.group_size,
        )
    }
}

impl Protocol {
    pub fn namespace(&self) -> &'static str {
        match self {
            Protocol::Entropy => "entropy",
            Protocol::Glacier => "glacier",
            Protocol::Replication => "replication",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RedirectSpec {
    pub put_url: String,
    pub get_url: String,
    pub block_size: usize,
}
