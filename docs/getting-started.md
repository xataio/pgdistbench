# Getting Started

This guide will get you running your first pgdistbench benchmark in minutes.

## Prerequisites

- Kubernetes cluster with kubectl configured
- Go 1.24+ (for building from source)

## Installation

### Build Binaries

Build the binaries:
```bash
make build
```

This creates two executables:
- `benchdriver` - The benchmark execution server
- `k8runner` - The Kubernetes orchestration tool

### Build and Deploy Container Image

The benchdriver runs as a container in Kubernetes, so you need to build and make the image available to your cluster.

#### For Minikube

Build the image directly in minikube's Docker environment:
```bash
# Point Docker to minikube's Docker daemon
eval $(minikube docker-env)

# Build the image
make docker_build

# Verify the image is available
docker images | grep benchdriver
```

#### For AWS EKS (or other cloud providers)

Build and push the image to a container registry:

1. **Build the image**:
   ```bash
   make docker_build
   ```

2. **Tag for your registry**:
   ```bash
   # Replace with your ECR repository URI
   docker tag benchdriver:latest 123456789012.dkr.ecr.us-west-2.amazonaws.com/benchdriver:latest
   ```

3. **Push to registry**:
   ```bash
   # Login to ECR (replace region as needed)
   aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-west-2.amazonaws.com

   # Push the image
   docker push 123456789012.dkr.ecr.us-west-2.amazonaws.com/benchdriver:latest
   ```

4. **Update runner configuration** to reference your image:
   ```kcl
   runners = {
     local = bench.Runner {
       name: "local"
       namespace: "benchmark"
       image: "123456789012.dkr.ecr.us-west-2.amazonaws.com/benchdriver:latest"
       imagePullPolicy: "Always"
       // ... rest of configuration
     }
   }
   ```

## Quick Start Walkthrough

Let's run a simple TPC-C benchmark using the included demo:

### 1. Navigate to Demo
```bash
cd demos/singledb_kcl
```

### 2. Deploy System Under Test
```bash
k8runner systems apply
```
This creates a PostgreSQL cluster in your Kubernetes cluster.

### 3. Deploy Benchmark Runners
```bash
k8runner runner apply
```
This deploys benchdriver pods that will execute the benchmark.

### 4. Run Your First Benchmark
```bash
k8runner exec prepare --wait local-tpcc-1
k8runner exec run --wait local-tpcc-1
k8runner exec cleanup --wait local-tpcc-1
```

**What each step does:**
- `prepare` - Creates TPC-C schema and loads test data
- `run` - Executes the TPC-C benchmark workload
- `cleanup` - Removes test data

## Understanding the Demo

### Configuration Structure
The demo uses a KCL configuration file (`main.k`) that defines:

- **Systems**: A PostgreSQL cluster named "local"
- **Runners**: Benchdriver pods that connect to the PostgreSQL cluster  
- **Benchmarks**: TPC-C benchmarks with different scale factors

### Results
Benchmark results are displayed in the terminal output, showing:
- Transaction throughput (TpmC)
- Response times
- Efficiency metrics

### Other Available Benchmarks
Try running other benchmarks from the demo:
```bash
k8runner exec prepare --wait local-tpch-1
k8runner exec run --wait local-tpch-1
k8runner exec cleanup --wait local-tpch-1
```

## Configuration Basics

### File Discovery
k8runner automatically finds configuration files:
- `main.k` or `main.kcl` (KCL format)
- `main.yaml` (YAML format)
- Use `-w <directory>` to specify a different working directory
- Use `-m <file>` to specify a specific configuration file

### YAML vs KCL
- **YAML**: Simple, familiar syntax - good for basic scenarios
- **KCL**: Type safety, validation, reusable modules - better for complex setups

Example YAML equivalent available in `demos/singledb_yaml/`.

## What's Next

- **Customize configurations**: See [Configuration Guide](./configuration.md)
- **Understand the architecture**: See [Architecture Guide](./architecture.md)
- **Try advanced scenarios**: Explore `demos/clusterload/` for cluster stress testing 