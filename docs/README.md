# pgdistbench Documentation

Welcome to the pgdistbench documentation. This guide will help you navigate the documentation based on your needs.

## Quick Navigation

### 🚀 **New to pgdistbench?**
Start here: [Getting Started Guide](./getting-started.md)
- Install and build pgdistbench
- Run your first benchmark in minutes
- Understand basic concepts

### ⚙️ **Need to configure benchmarks?**
Go to: [Configuration Guide](./configuration.md)
- Systems, runners, and benchmark configuration
- YAML and KCL examples
- Common configuration patterns

### 🏗️ **Want to understand how it works?**
Read: [Architecture Guide](./architecture.md)
- System architecture and components
- Integration details

## Documentation Structure

| Document | Purpose | Audience |
|----------|---------|----------|
| [Getting Started](./getting-started.md) | Quick start guide with hands-on walkthrough | New users, quick setup |
| [Configuration](./configuration.md) | Complete configuration reference and examples | All users configuring benchmarks |
| [Architecture](./architecture.md) | Technical architecture and design details | Advanced users, developers |

## Additional Resources

- **Examples**: See the `demos/` directory for complete working examples
  - `demos/singledb_kcl/` - Single database with KCL configuration
  - `demos/singledb_yaml/` - Single database with YAML configuration
  - `demos/clusterload/` - Kubernetes cluster stress testing
- **API Reference**: See `api/benchdriverapi/` package for detailed type definitions
- **Source Code**: Explore `cmd/`, `internal/`, and `pkg/` directories
