use memmap2::MmapOptions;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufWriter, Cursor, Write},
    sync::{Arc, Mutex},
    thread
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

    fn mean(&self) -> f32 { ((self.total / self.count as f32) * 10.0).round() / 10.0 }
}

fn main() {
    // let now = Instant::now();

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

    let mut current_pos = 0;
    while current_pos < file_len {
        let end = if current_pos + CHUNK_SIZE > file_len { file_len } else { current_pos + CHUNK_SIZE };

        // Look ahead for a newline if we're not at the end of file
        let mut chunk_end = end;
        if end < file_len {
            // Find the next newline after the chunk boundary
            let next_chunk = &file_arc[end..std::cmp::min(end + 48, file_len)];
            if let Some(newline_pos) = next_chunk.iter().position(|&b| b == b'\n') {
                chunk_end = end + newline_pos + 1; // Include the newline
            }
        }

        let file_local = file_arc.clone();
        let results_local = results.clone();

        let start = current_pos;
        let end = chunk_end;

        pool.execute(move || {
            let mut local_map: HashMap<String, Measurement> = HashMap::new();
            let mut reader = Cursor::new(&file_local[start..end]);

            let mut buffer = String::new();
            while let Ok(bytes_read) = reader.read_line(&mut buffer) {
                if bytes_read == 0 {
                    break;
                }

                let parts: Vec<&str> = buffer.trim_end().split(';').collect();

                if let (Some(location), Ok(temperature)) = (parts.get(0), fast_float::parse(parts.get(1).unwrap())) {
                    local_map
                        .entry(location.to_string())
                        .and_modify(|e| e.update(temperature))
                        .or_insert_with(|| Measurement::new(temperature));
                }

                buffer.clear();
            }

            results_local.lock().unwrap().push(local_map)
        });

        current_pos = chunk_end;
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

    // Sort the results
    let mut cities: Vec<_> = global_map.iter().collect();
    cities.sort_by(|a, b| a.0.cmp(b.0));

    // Print the results
    let mut stdout = BufWriter::new(io::stdout().lock());
    cities.iter().for_each(|(location, temperature)| {
        let mut buffer: [u8; 48] = [0u8; 48];

        let location_bytes = location.as_bytes();
        buffer[..location_bytes.len()].copy_from_slice(location_bytes);
        let mut pos = location_bytes.len();

        buffer[pos] = b'=';
        pos += 1;

        let mut buf = ryu::Buffer::new();
        let min = buf.format_finite(temperature.min);
        buffer[pos..pos + min.len()].copy_from_slice(min.as_bytes());
        pos += min.len();

        buffer[pos] = b'/';
        pos += 1;

        let mean = buf.format_finite(temperature.mean());
        buffer[pos..pos + mean.len()].copy_from_slice(mean.as_bytes());
        pos += mean.len();

        buffer[pos] = b'/';
        pos += 1;

        let max = buf.format_finite(temperature.max);
        buffer[pos..pos + max.len()].copy_from_slice(max.as_bytes());
        pos += max.len();

        buffer[pos] = b',';
        pos += 1;

        stdout.write_all(&buffer[..pos]).unwrap();
    });

    // Ensure the writter is flushed
    stdout.flush().unwrap();

    // stdout
    //     .write_all(format!("\nTotal execution time is: {:?}\n", now.elapsed()).as_bytes())
    //     .unwrap();
}
