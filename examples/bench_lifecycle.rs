use std::{
    iter::repeat_with,
    time::{Duration, Instant},
};

use bytes::Bytes;
use entropy_app::block::{Block, Parameters};
use rand::{
    distr::{Distribution, WeightedIndex},
    rngs::StdRng,
    RngCore as _, SeedableRng,
};

fn main() {
    let mut rng = StdRng::seed_from_u64(117418);
    // let mut lines = String::new();
    let key = entropy_app::generate_signing_key(&mut rng);

    let parameters = Parameters {
        // chunk_size: 1600,
        // k: 160 << 10,
        chunk_size: 6400,
        k: 640 << 10,
    };
    // let weights = p_degree(parameters.k);
    let weights = entropy_app::block::distr::p_degree_ideal(parameters.k);
    let degree_distr = WeightedIndex::new(weights).expect("valid weights");

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
    let mut scratch_packet: Option<entropy_app::block::Packet> = None;
    let mut count = 0;
    let mut periodically = Period(Instant::now());
    let recovered_block = loop {
        let start = Instant::now();
        let packet = block
            .generate_with_degree(degree_distr.sample(&mut rng) + 1, &mut rng)
            .expect("can generate packet");
        encode += start.elapsed();

        let start = Instant::now();
        packet
            .verify(&key.verifying_key(), &parameters)
            .expect("can verify");
        verify += start.elapsed();

        let start = Instant::now();
        let can_recover = if let Some(scratch_packet) = &mut scratch_packet {
            scratch_packet.merge(packet);
            scratch_packet.can_recover(&parameters)
        } else {
            let b = packet.can_recover(&parameters);
            scratch_packet = Some(packet);
            b
        };
        if can_recover {
            let block = scratch_packet
                .take()
                .unwrap()
                .recover(&parameters)
                .expect("can recover");
            decode += start.elapsed();
            break block;
        } else {
            decode += start.elapsed()
        }
        count += 1;
        periodically.run(|| eprint!("{count}\r"))
    };
    eprintln!();
    println!("count {count}");
    println!("chunk count {}", recovered_block.chunks.len());
    println!("encode {encode:?} verify {verify:?} decode {decode:?}");
}

struct Period(Instant);

impl Period {
    fn run(&mut self, task: impl FnOnce()) {
        if self.0.elapsed() > Duration::from_secs(1) {
            task();
            self.0 = Instant::now()
        }
    }
}
