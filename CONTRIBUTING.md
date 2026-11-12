# Contributing

Contributions to PostgreSQL Concurrency Tester are welcome. Issues and pull requests should focus on reproducible PostgreSQL concurrency experiments, accurate measurement, safety, or clear documentation.

## Development setup

Install Rust 1.85 or newer and Docker Compose. Clone the repository and start the disposable PostgreSQL lab:

```sh
git clone https://github.com/ranajahanzaib/postgres-concurrency-tester.git
cd postgres-concurrency-tester
docker compose -f lab/compose.yaml up -d --wait
cargo run -- lab init --seed 42
```

Before submitting a change, run the local checks:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo build --release
```

For changes to database behavior, run at least one relevant workload against the local lab and include the command and observed outcome in the pull request. Do not include credentials, private reports, or production data in issues or pull requests. Stop the lab when finished with `docker compose -f lab/compose.yaml down -v`; this removes its local database volume.

## Pull requests

Describe the behavior changed and why. Include tests or reproduction steps, note any changes to the CLI or report format, and update the README when user-visible behavior changes. Keep benchmark claims tied to measured results and describe the machine and configuration used. Do not present local measurements as universal performance guarantees.

For substantial changes, open an issue first to discuss scope and compatibility. Be respectful in review discussions and follow the [Code of Conduct](CODE_OF_CONDUCT.md).
