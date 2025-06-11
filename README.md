# pgdistbench

A distributed PostgreSQL benchmarking tool designed to run comprehensive database performance tests at scale in Kubernetes environments.

## Overview

pgdistbench enables you to:
- **Benchmark PostgreSQL instances** (TPC-C, TPC-H, CHBench)
- **Scale testing across multiple replicas** for distributed load generation
- **Test Kubernetes-native PostgreSQL deployments** (CloudNativePG, static clusters)
- **Stress test entire clusters** with dynamic PostgreSQL instance lifecycle management
- **Configure complex scenarios** using KCL (KusionStack Configuration Language)

## Architecture

The tool consists of two main components:

- **Bench Driver**: An HTTP server that executes the actual benchmarks against the system under test
- **K8 Runner**: A CLI tool that manages benchmark deployments, configures systems under test, and coordinates multiple bench driver replicas

## Quick Start

### Prerequisites

- Kubernetes cluster
- Go 1.24+ (for building from source)
- kubectl configured for your cluster

### Installation

Build the binaries:
```bash
make build
```

This creates two executables:
- `benchdriver` - The benchmark execution server
- `k8runner` - The Kubernetes orchestration tool

### Basic Usage

1. **Configure your benchmark** using KCL (see `demos/` for examples):
```bash
# Single database benchmark
cd demos/singledb_kcl
```

2. **Deploy systems under test**:
```bash
k8runner systems apply
```

3. **Deploy benchmark runners**:
```bash
k8runner runner apply
```

4. **Execute benchmarks**:
```bash
k8runner exec prepare --wait tpcc-1
k8runner exec run --wait tpcc-1
k8runner exec cleanup --wait tpcc-1
```

## Supported Benchmarks

- **TPC-C**: OLTP benchmark simulating order processing workloads
- **TPC-H**: OLAP benchmark with complex analytical queries  
- **CHBench**: Mixed OLTP/OLAP workload combining TPC-C and TPC-H
- **K8Stress**: Kubernetes-native stress testing with dynamic PostgreSQL lifecycle management

## Supported Systems Under Test

- **CloudNativePG (CNPG)**: Kubernetes-native PostgreSQL clusters
- **Static Clusters**: Pre-existing PostgreSQL instances
- **Kubernetes Clusters**: Direct cluster stress testing

## Examples & Demos

Explore the `demos/` directory for complete examples:

- [`demos/singledb_kcl/`](./demos/singledb_kcl/) - Single database benchmarking with KCL configuration
- [`demos/singledb_yaml/`](./demos/singledb_yaml/) - Single database benchmarking with YAML configuration  
- [`demos/clusterload/`](./demos/clusterload/) - Kubernetes cluster stress testing

## Configuration

Benchmarks can be configured using YAML or [KCL](https://kcl-lang.io/). KCL provides:
- Type safety and validation
- Modular and reusable configurations
- Integration with Kubernetes resources

See the [`scripts/kcl/k8dbbench/`](./scripts/kcl/k8dbbench/) directory for the KCL configuration schema definitions.

## Documentation

For detailed documentation, see the [`docs/`](./docs/) directory:

- [Documentation Hub](./docs/) - Start here for navigation and overview
- [Getting Started Guide](./docs/getting-started.md) - Quick start walkthrough
- [Architecture Guide](./docs/architecture.md) - Technical architecture and design
- [Configuration Reference](./docs/configuration.md) - Complete configuration guide

## Building

```bash
# Build both binaries
make build

# Build individual components
go build -o benchdriver ./cmd/benchdriver
go build -o k8runner ./cmd/k8runner

# Build driver image via docker
make docker_build
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request
