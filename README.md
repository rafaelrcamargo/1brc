# 1️⃣🐝🏎️ The One Billion Row Challenge

A fun exploration of how quickly 1B rows from a text file can be aggregated with Rust.

## Quick one-liner, to get started

```bash
cargo build --release && ./target/release/generator && hyperfine --warmup 1 --runs 3 './target/release/aggregator'
```
