# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial Rust CLI for running PostgreSQL inventory-hotspot, pool-starvation, and indexed-read workloads.
- Docker Compose laboratory with separate setup, workload, and monitoring roles.
- JSON and Markdown run reports, a local report dashboard/API, run comparison, and bounded concurrency matrices.
- CI checks for formatting, linting, unit tests, release builds, and real PostgreSQL smoke workloads.
