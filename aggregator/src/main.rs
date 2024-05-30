#![feature(bufread_skip_until)]

use memmap2::MmapOptions;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufWriter, Cursor, Write},
    sync::{Arc, Mutex},
    thread,
    time::Instant
};
use threadpool::ThreadPool;

const CHUNK_SIZE: usize = 1 << 20;

#[derive(Clone)]
struct Measurement {
    min: f32,
    max: f32,
    total: f32,
    count: usize
}

impl Measurement {
    fn new(temperature: f32) -> Self {
        Self {
            min: temperature,
            max: temperature,
            total: temperature,
            count: 1
        }
    }

    fn update(&mut self, temperature: f32) {
        self.min = self.min.min(temperature);
        self.max = self.max.max(temperature);
        self.total += temperature;
        self.count += 1;
    }

    fn mean(&self) -> f32 { self.total / self.count as f32 }
}

fn main() {
    let now = Instant::now();

    let threads: usize = thread::available_parallelism().unwrap().into();

    let file = unsafe {
        MmapOptions::new()
            .map(&File::open("data/measurements-1000000000.txt").unwrap())
            .unwrap()
    };

    let file_len = file.len();
    let file_arc = Arc::new(file);

    let pool = ThreadPool::new(threads);

    let results = Arc::new(Mutex::new(Vec::new()));

    for i in 0..file_len.div_ceil(CHUNK_SIZE) {
        let start = i * CHUNK_SIZE;
        let end = if start + CHUNK_SIZE > file_len { file_len } else { start + CHUNK_SIZE };

        let file_local = file_arc.clone();
        let results_local = results.clone();

        pool.execute(move || {
            let mut local_map: HashMap<String, Measurement> = HashMap::new();
            let mut reader = Cursor::new(&file_local[start..end]);

            if i > 0 {
                reader.skip_until(b'\n').unwrap();
            }

            let mut buffer = String::new();
            while let Ok(bytes_read) = reader.read_line(&mut buffer) {
                if bytes_read == 0 {
                    break;
                }

                let parts: Vec<&str> = buffer.trim_end().split(';').collect();
                if let (Some(location), Ok(temperature)) = (parts.get(0), parts.get(1).unwrap_or(&"").parse::<f32>()) {
                    local_map
                        .entry(location.to_string())
                        .and_modify(|e| e.update(temperature))
                        .or_insert_with(|| Measurement::new(temperature));
                }

                buffer.clear();
            }

            results_local.clone().lock().unwrap().push(local_map)
        });
    }

    pool.join();

    let mut global_map: HashMap<String, Measurement> = HashMap::new();
    let results = results.lock().unwrap();

    for local_map in results.iter() {
        for (city, stats) in local_map {
            global_map
                .entry(city.to_owned())
                .and_modify(|e| {
                    e.total += stats.total;
                    e.count += stats.count;
                    e.min = e.min.min(stats.min);
                    e.max = e.max.max(stats.max);
                })
                .or_insert_with(|| stats.clone());
        }
    }

    // Sort and print results
    let cities: Vec<_> = global_map.iter().collect();

    let mut stdout = BufWriter::new(io::stdout().lock());

    stdout.write_all(b"{").unwrap();
    stdout
        .write_all(
            cities
                .iter()
                .collect::<Vec<_>>()
                .par_iter()
                .map(|(location, temperature)| {
                    format!(
                        "{}={}/{:.1}/{}, ",
                        location,
                        temperature.min,
                        temperature.mean(),
                        temperature.max
                    )
                    .as_bytes()
                    .to_owned()
                })
                .collect::<Vec<_>>()
                .concat()
                .as_slice()
        )
        .unwrap();
    stdout.write_all(b"}\n").unwrap();

    stdout
        .write_all(format!("\nTotal execution time is: {:?}\n", now.elapsed()).as_bytes())
        .unwrap();

    stdout.flush().unwrap();
}
