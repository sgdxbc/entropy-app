use std::{
    iter::repeat_with,
    time::{Duration, Instant},
};

use bytes::Bytes;
use entropy_app::block::{Block, Parameters};
use rand::{rngs::StdRng, RngCore as _, SeedableRng};

fn main() {
    let mut rng = StdRng::seed_from_u64(117418);
    let mut lines = String::new();
    let key = entropy_app::generate_signing_key(&mut rng);

    let parameters = Parameters {
        chunk_size: 1600,
        k: 640 << 10,
    };

    println!("Fill random block bytes");
    let chunks = repeat_with(|| {
        let mut buf = vec![0; parameters.chunk_size];
        rng.fill_bytes(&mut buf);
        Bytes::from(buf)
    })
    .take(parameters.k)
    .collect::<Vec<_>>();

    println!("Initialize block");
    let start = Instant::now();
    let block = Block::new(chunks, &key);
    println!("{:?}", start.elapsed());

    let mut encode = Duration::ZERO;
    let mut verify = Duration::ZERO;
    let mut decode = Duration::ZERO;
}
