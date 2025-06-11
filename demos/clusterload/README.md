# K8s CNPG load test

This demo showcases a load testing approach for a Kubernetes cluster utilizing the CNPG operator. The process involves randomly starting and stopping databases to simulate real-world scenarios. During the test, TPC-C benchmarks are executed against the active databases to evaluate performance metrics. For detailed configuration settings, please refer to the `main.k` file.

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

### 1. Setup System under Test

- We test the k8s cluster directly => We need additional permissions
- from project root apply configs/stressroles.yaml to k8s cluster

```
kubectl apply -f ./configs/stressroles.yaml
```

### 2. Setup Test Runner

```bash
k8runner -c $CONTEXT runner apply cluster
```

### 3. Start tests

```
k8runner -c $CONTEXT exec run loadtest
```

While the test is active we can query activity metrics (which are normally exposed via Prometheus):

```
k8runner -c $CONTEXT runner metrics cluster
```

### 4. Stop tests

```
k8runner -c $CONTEXT runner cancel cluster
```

## Teardown

```
k8runner -c $CONTEXT runner delete cluster
```
