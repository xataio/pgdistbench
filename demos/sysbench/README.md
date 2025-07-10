# Sysbench TPCC Demo

This demo shows how to use the `script` benchmark type in pgdistbench to run a sysbench-tpcc workload using the Percona sysbench-tpcc scripts and a custom runner script (`tpcc-runner.sh`).

## Prerequisites

- Kubernetes cluster (e.g., minikube)
- kubectl configured
- CloudNativePG operator installed (`kubectl krew install cnpg`)
- Docker (for building the benchdriver image)
- pgdistbench source code (this repo)

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

### 3. Build Docker Image

Make minikube docker available:

```bash
eval $(minikube docker-env)
```

Build the benchdriver Docker image (from project root):

```bash
make build docker_build
```

## Running the Sysbench Demo

### 1. Apply System Under Test (CNPG Cluster)

The system is defined in `main.k` as a CNPG cluster named `local` in the `benchmark` namespace.

Apply the system:

```bash
k8runner -c $CONTEXT systems create local
```

Check system status:

```bash
k8runner -c $CONTEXT systems list local
```

### 2. Apply the Runner

```bash
k8runner -c $CONTEXT runner apply local
```

### 3. Execute the Sysbench TPCC Benchmark

#### Prepare Database

```bash
k8runner -c $CONTEXT exec prepare --wait sysbench_tpcc
```

#### Run Benchmark

```bash
k8runner -c $CONTEXT exec run sysbench_tpcc
```

#### Get Results

```bash
k8runner -c $CONTEXT exec results sysbench_tpcc
```

#### Cleanup Database

```bash
k8runner -c $CONTEXT exec cleanup sysbench_tpcc
```

### 4. Teardown

Delete the runner:

```bash
k8runner -c $CONTEXT runner delete local
```

Delete the system under test:

```bash
k8runner -c $CONTEXT systems delete local
```

## Environment Variables

The following environment variables are automatically provided to your script commands by the system:

- `PGHOST`
- `PGPORT`
- `PGUSER`
- `PGPASS`
- `PGSSLMODE`
- `PGDATABASE`

Reference them in your script commands as shown in `main.k`.

## Notes

- The sysbench-tpcc scripts and `tpcc-runner.sh` must be present in the Docker image as described in the main project documentation.
- You can adjust the CNPG cluster spec, sysbench parameters, and aggregation fields in `main.k` as needed.

## References

- [Percona sysbench-tpcc](https://github.com/Percona-Lab/sysbench-tpcc)
- [Script Benchmark Documentation](../../docs/configuration.md#script-benchmarks)
