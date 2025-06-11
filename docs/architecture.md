# Architecture

This document provides a comprehensive overview of pgdistbench's architecture, components, and design patterns.

## System Overview

pgdistbench is designed as a distributed benchmarking system with clear separation between orchestration and execution:

```
┌─────────────────┐    ┌──────────────────────────────────────┐
│                 │    │           Kubernetes Cluster        │
│   k8runner      │    │                                      │
│   (CLI Tool)    │────┤  ┌─────────────┐  ┌─────────────┐   │
│                 │    │  │ Bench       │  │ Bench       │   │
└─────────────────┘    │  │ Driver      │  │ Driver      │   │
                       │  │ Pod 1       │  │ Pod N       │   │
                       │  └─────────────┘  └─────────────┘   │
                       │         │                │          │
                       │         ▼                ▼          │
                       │  ┌─────────────┐  ┌─────────────┐   │
                       │  │ PostgreSQL  │  │ PostgreSQL  │   │
                       │  │ Instance 1  │  │ Instance N  │   │
                       │  └─────────────┘  └─────────────┘   │
                       └──────────────────────────────────────┘
```

### Core Design Principles

- **Separation of Concerns**: k8runner handles orchestration, benchdrivers handle execution
- **Kubernetes-Native**: Leverages Kubernetes for deployment, scaling, and resource management
- **Horizontal Scalability**: Multiple benchdriver replicas coordinate to generate distributed load
- **Configuration-Driven**: Declarative configuration using YAML or KCL
- **Stateless Execution**: Benchdrivers are stateless, enabling easy scaling and recovery

## Components

### K8 Runner (CLI Orchestrator)

The k8runner is a CLI tool that manages the entire benchmark lifecycle:

**Responsibilities:**
- Parse and validate configuration files (YAML/KCL)
- Deploy and manage Kubernetes resources
- Coordinate multiple benchdriver instances
- Aggregate results from distributed execution

**Key Features:**
- **Configuration Management**: Supports both YAML and KCL configuration formats
- **Resource Orchestration**: Creates and manages Kubernetes deployments, services, and other resources
- **Multi-Replica Coordination**: Automatically discovers and coordinates with multiple benchdriver replicas
- **Lifecycle Management**: Handles prepare, run, and cleanup phases across all replicas

### Benchdriver (Execution Engine)

The benchdriver is an HTTP server that executes the actual benchmarks:

**Responsibilities:**
- Connect to PostgreSQL instances
- Execute benchmark workloads (TPC-C, TPC-H, CHBench, K8Stress)
- Collect performance metrics and statistics
- Provide HTTP API for remote control

**HTTP API Design:**
- **RESTful Interface**: Task-based endpoints for different operations
- **Asynchronous Execution**: Long-running benchmarks execute asynchronously
- **Status Reporting**: Real-time status and progress reporting
- **Result Collection**: Structured performance data and statistics

## System Under Test (SUT)

The System Under Test represents the PostgreSQL environment being benchmarked:

### SUT Types

**CloudNativePG (CNPG) Clusters:**
- Kubernetes-native PostgreSQL clusters
- Dynamic provisioning and configuration
- Integrated with Kubernetes resource management
- Supports high availability and scaling scenarios

**Static Clusters:**
- Pre-existing PostgreSQL instances
- External database connections
- Useful for testing existing infrastructure
- Minimal Kubernetes resource requirements

**Kubernetes Clusters:**
- Direct cluster stress testing
- Dynamic PostgreSQL instance lifecycle

### Multi-Replica Coordination

When multiple benchdriver replicas are deployed:

1. **Discovery**: k8runner discovers all available benchdriver pods
2. **Coordination**: Sends identical commands to all replicas simultaneously
4. **Aggregation**: Collects and combines results from all replicas

## Scaling Patterns

### Horizontal Scaling

**Runner Replicas:**
- Configure `spec.replicas` in runner configuration
- Each replica runs independently but coordinates through k8runner
- Linear scaling of load generation capacity

**Resource Considerations:**
- CPU: Each benchdriver replica needs sufficient CPU for database connections
- Memory: Memory usage scales with connection count and result buffering
- Network: Database connections and HTTP API traffic
- Storage: Minimal storage requirements (logs and temporary data)

### Performance Characteristics

**Scaling Benefits:**
- Increased concurrent database connections
- Higher throughput and load generation
- Better simulation of distributed client scenarios

**Scaling Limits:**
- Database server capacity (connections, CPU, I/O)
- Network bandwidth between benchdrivers and database
- Kubernetes cluster resource limits

### Best Practices

**Resource Sizing:**
- Start with 1 replicas and scale based on database capacity
- Monitor database server metrics during scaling
- Ensure sufficient network bandwidth

**Configuration Patterns:**
- Use resource requests/limits to ensure predictable performance
- Configure appropriate connection counts per replica
- Consider database connection limits when scaling

## Configuration Management

### YAML vs KCL

**YAML Configuration:**
- Simple, familiar format
- Good for basic scenarios
- Limited validation and reusability

**KCL Configuration:**
- Type safety and validation
- Modular and reusable configurations
- Advanced templating and logic
- Better for complex scenarios

### Configuration Schema

The configuration schema is based on the benchdrivers HTTP API. KCL schemas definition can be found in `scripts/kcl/k8dbbench/`, which includes:

- **Systems**: Define PostgreSQL instances and clusters
- **Runners**: Configure bench driver deployments
- **Benchmarks**: Specify benchmark parameters and execution

## Benchdriver HTTP API

The benchdriver exposes a RESTful API that k8runner uses to control benchmark execution:

### Base Endpoints

- `GET /` - List all available routes
- `GET /status` - Get worker status and last task result
- `GET /healthz` - Health check endpoint
- `POST /work/stop` - Stop currently running task

### Benchmark-Specific Endpoints

Each benchmark type has its own endpoint group under `/work/`:

**TPC-C**: `/work/tpcc/`
- `POST /work/tpcc/prepare` - Prepare database with TPC-C schema and data
- `POST /work/tpcc/run` - Execute TPC-C benchmark
- `POST /work/tpcc/cleanup` - Clean up TPC-C data

**TPC-H**: `/work/tpch/`
- `POST /work/tpch/prepare` - Prepare database with TPC-H schema and data
- `POST /work/tpch/run` - Execute TPC-H queries
- `POST /work/tpch/cleanup` - Clean up TPC-H data

**CHBench**: `/work/chbench/`
- `POST /work/chbench/prepare` - Prepare database for mixed workload
- `POST /work/chbench/run` - Execute CHBench mixed OLTP/OLAP workload
- `POST /work/chbench/cleanup` - Clean up CHBench data

**K8Stress**: `/work/k8stress/`
- `POST /work/k8stress/prepare` - Prepare for Kubernetes stress testing
- `POST /work/k8stress/run` - Execute K8Stress benchmark
- `POST /work/k8stress/cleanup` - Clean up stress test resources

### API Integration

- **Configuration Translation**: k8runner converts KCL/YAML configurations to JSON request bodies
- **Multi-Replica Coordination**: k8runner sends identical requests to all benchdriver replicas
- **Result Aggregation**: k8runner collects and combines responses from all replicas
- **Error Handling**: HTTP status codes and error responses for proper error handling