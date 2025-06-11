# Single Database Benchmark Demo

This demo uses CNPG and the benchmark tooling to create a test database and run benchmarks against it.

The setup for the systems, runner, and benchmarks is defined in the `main.yaml`
file. This file contains the configuration necessary to create a CNPG base
PostgreSQL cluster and run the specified benchmarks.

## Prerequisites

- Kubernetes cluster (demo uses minikube)
- kubectl configured
- kubectl cnpg plugin (`kubectl krew install cnpg`)

## Setup

### 1. Kubernetes Environment

Start minikube:
```bash
minikube start --cpus 2 --driver qemu --network socket_vmnet --disk-size 20gb
```

Configure context:
```bash
export CONTEXT="minikube"  # Get available contexts: kubectl config get-contexts
```

Create namespace:
```bash
kubectl create namespace --context $CONTEXT benchmark
```

### 2. Install CNPG Operator

Install the operator:
```bash
kubectl cnpg install generate | kubectl --context $CONTEXT create -f -
```

Create a test cluster:
```bash
kubectl apply --context $CONTEXT -f - <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: testing
spec:
  storage:
    size: 1Gi
EOF
```

Test the connection:
```bash
kubectl cnpg --context $CONTEXT psql testing
```

Cleanup test cluster:
```bash
kubectl delete --context $CONTEXT -f - <<EOF
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: testing
EOF
```

### 3. Build Benchmark Tools

Make minikube docker available:
```bash
eval $(minikube docker-env)
```

Build from project root:
```bash
make build docker_build
```

Update PATH:
```bash
export PATH="$(pwd):$PATH"
```

## Running Benchmarks

### 1. Setup System Under Test

Create database instance (see systems.local in main.yaml):
```bash
k8runner systems create local
```

Verify system status:
```bash
k8runner -c $CONTEXT systems list local
```

### 2. Setup Test Runner

```bash
k8runner -c $CONTEXT runner apply local
```

### 3. Execute Benchmarks

#### TPC-C

Prepare:
```bash
k8runner -c $CONTEXT exec prepare --wait local-tpcc-1
```

Run:
```bash
k8runner -c $CONTEXT exec run local-tpcc-1
```

Get results (can be called multiple times):
```bash
k8runner -c $CONTEXT exec results local-tpcc-1
```

Cleanup:
```bash
k8runner -c $CONTEXT exec cleanup local-tpcc-1
```

#### TPC-H

Prepare:
```bash
k8runner -c $CONTEXT exec prepare --wait local-tpch-1
```

Run and wait for results:
```bash
k8runner -c $CONTEXT exec run --wait local-tpch-1
```

Cleanup:
```bash
k8runner -c $CONTEXT exec cleanup local-tpch-1
```

### 4. Teardown

Delete test runner

```bash
k8runner -c $CONTEXT runner delete local
```

Delete system under test

```bash
k8runner -c $CONTEXT systems delete local
```

