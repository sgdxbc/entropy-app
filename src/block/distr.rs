use std::collections::HashSet;

use rand::{
    distr::{Uniform, WeightedIndex},
    prelude::Distribution as _,
    Rng,
};

pub fn p_degree_ideal(k: usize) -> Vec<f64> {
    let mut ps = vec![1. / k as f64];
    for i in 2..=k {
        ps.push(1. / (i * (i - 1)) as f64)
    }
    ps
}

pub fn p_degree(k: usize) -> Vec<f64> {
    let delta = 0.5;
    let c = 0.01;

    let mut tau = p_degree_ideal(k);
    let k = k as f64;
    let s = c * (k / delta).ln() * k.sqrt();
    eprintln!("k {k} s {s} k/s {}", (k / s).floor());
    for (i, p) in tau.iter_mut().enumerate() {
        let d = (i + 1) as f64;
        if d == (k / s).floor() {
            *p += s / k * (s / delta).ln();
            break;
        }
        *p += s / k * (1. / d)
    }
    tau
}

#[derive(Debug)]
pub struct PacketDistr(WeightedIndex<f64>, Uniform<usize>);

impl PacketDistr {
    pub fn new(k: usize) -> Self {
        Self(
            WeightedIndex::new(p_degree(k)).expect("valid weights"),
            Uniform::new(0, k).expect("valid range"),
        )
    }

    pub fn sample(&self, mut rng: impl Rng) -> HashSet<usize> {
        let d = self.0.sample(&mut rng) + 1;
        let mut fragment = HashSet::new();
        while fragment.len() < d {
            fragment.insert(self.1.sample(&mut rng));
        }
        fragment
    }

    pub fn write_degree_distr(&self, mut write: impl std::fmt::Write) {
        for (i, weight) in self.0.weights().enumerate() {
            writeln!(&mut write, "{},{}", i + 1, weight / self.0.total_weight()).expect("can write")
        }
    }
}
