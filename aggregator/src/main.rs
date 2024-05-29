use memmap2::MmapOptions;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Write}
};

const PAGE_SIZE: usize = 3250;

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
    // let cores: usize = thread::available_parallelism().unwrap().into();
    // println!("Available cores: {}", cores);

    let mut accumulator: HashMap<String, Measurement> = HashMap::new();

    let file = File::open("assets/measurements-1000000000.txt").unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    let total = mmap.len();

    let mut read = 0;

    while read < total {
        let range = read + (if read + PAGE_SIZE <= total { PAGE_SIZE } else { total - read });
        let chunk = String::from_utf8_lossy(&mmap[read..range]);

        if let Some(pos) = chunk.rfind('\n') {
            chunk[..pos].split('\n').for_each(|line| {
                let mut parts = line.split(';');
                let location = parts.next().unwrap().to_string();
                let temperature = parts.next().unwrap().parse().unwrap();

                accumulator
                    .entry(location)
                    .and_modify(|measurement| measurement.update(temperature))
                    .or_insert(Measurement::new(temperature));
            });

            read += pos + 1;
        }
    }

    let mut stdout = io::stdout().lock();
    stdout.write_all(b"{").unwrap();
    accumulator.iter().for_each(|(location, measurement)| {
        stdout
            .write_all(
                format!(
                    "{}={}/{:.1}/{}, ",
                    location,
                    measurement.min,
                    measurement.mean(),
                    measurement.max
                )
                .as_bytes()
            )
            .unwrap();
    });
    stdout.write_all(b"}\n").unwrap();
}
