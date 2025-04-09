


## Perf Benchmarking Script

This project benchmarks insert and query performance across multiple schemas.

Requirements
- Poetry
- Rust
- sqlx-cli installed:

```bash
cargo install sqlx-cli --no-default-features --features postgres
```

Make sure DATABASE_URL is set in your environment or in `run.py` for sqlx commands to work 

Setup
1.	Install dependencies with Poetry:
```bash
poetry install
```

2.	Run the benchmark script:
```bash
poetry run perf
```

Notes
- The script assumes configured DbClient instances for benchmarking.
- You can extend or configure the targets inside the script.

