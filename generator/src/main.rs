mod dataset;

use rand::{rngs::ThreadRng, seq::SliceRandom};
use rand_distr::{Distribution, Normal};
use rayon::prelude::*;
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    sync::{Arc, Mutex},
    time::Instant
};

fn generate_temperature(rng: &mut ThreadRng, mean_temperature: f32) -> f32 {
    Normal::new(mean_temperature, 10.0).unwrap().sample(rng)
}

fn generate_measurements<const N: usize>(lines: usize, cities: [(&str, f32); N]) {
    let file = File::create(format!("data/measurements-{}.txt", lines)).expect("Unable to create file");
    let writer = Arc::new(Mutex::new(BufWriter::new(file)));

    (0..(lines / 100000)).into_par_iter().for_each(|_| {
        let lines = (0..100000)
            .into_par_iter()
            .map(|_| {
                let mut rng = rand::thread_rng();
                let city = cities.choose(&mut rng).unwrap();

                format!("{};{:.1}\n", city.0, generate_temperature(&mut rng, city.1))
                    .as_bytes()
                    .to_owned()
            })
            .collect::<Vec<Vec<u8>>>()
            .concat();

        writer
            .lock()
            .unwrap()
            .write_all(&lines)
            .expect("Unable to write to file");
    });

    writer
        .lock()
        .unwrap()
        .flush()
        .expect("Unable to flush file");
}

fn main() {
    fs::create_dir_all("data").expect("Unable to create directory");

    let now = Instant::now();
    println!("Generating measurements...\n");

    for lines in [100000, 1000000, 10000000, 100000000, 1000000000].iter() {
        let now = Instant::now();

        generate_measurements(*lines, dataset::CITIES);

        let elapsed = now.elapsed();
        println!("Done generating {} lines in: {:.4?}", lines, elapsed);
    }

    println!("\nDone generating all measurements in: {:.4?}", now.elapsed());
}
