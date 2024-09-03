use std::iter::repeat_with;

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use ed25519_dalek::SigningKey;
use entropy_app::block::{Block, Parameters};
use rand::{thread_rng, RngCore as _};

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = thread_rng();

    let parameters = Parameters {
        chunk_size: 1600,
        k: 640 << 10,
        // k: 640,
    };
    let chunks = repeat_with(|| {
        let mut buf = vec![0; parameters.chunk_size];
        rng.fill_bytes(&mut buf);
        Bytes::from(buf)
    })
    .take(parameters.k)
    .collect::<Vec<_>>();
    let key = SigningKey::generate(&mut rng);

    // c.bench_function("construct block", |b| {
    //     b.iter(|| black_box(Block::new(chunks.clone(), &key)))
    // });

    let block = Block::new(chunks.clone(), &key);

    let mut group = c.benchmark_group("generate fixed k");
    for degree in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(degree),
            &degree,
            |b, &degree| b.iter(|| black_box(block.generate_with_degree(degree, &mut rng))),
        );
    }
    drop(group);

    let mut group = c.benchmark_group("verify fixed k");
    for degree in [1, 2, 4, 8] {
        group.bench_function(BenchmarkId::from_parameter(degree), |b| {
            let packet = block.generate_with_degree(degree, &mut rng).unwrap();
            b.iter(|| black_box(packet.verify(&key.verifying_key(), &parameters)))
        });
    }
    drop(group);

    let mut group = c.benchmark_group("generate fixed degree");
    for x in [16, 32, 64] {
        let p = Parameters {
            chunk_size: (1 << 10) / x * 100,
            k: (x * 10) << 10,
        };
        let block = Block::generate(&mut rng, &key, &p);
        group.bench_function(BenchmarkId::from_parameter(x * 10), |b| {
            b.iter(|| black_box(block.generate_with_degree(1, &mut rng)))
        });
    }
    drop(group);

    let mut group = c.benchmark_group("verify fixed degree");
    for x in [16, 32, 64] {
        let p = Parameters {
            chunk_size: (1 << 10) / x * 100,
            k: (x * 10) << 10,
        };
        let block = Block::generate(&mut rng, &key, &p);
        group.bench_function(BenchmarkId::from_parameter(x * 10), |b| {
            let packet = block.generate_with_degree(1, &mut rng).unwrap();
            b.iter(|| black_box(packet.verify(&key.verifying_key(), &parameters)))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
