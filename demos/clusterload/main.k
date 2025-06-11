import k8dbbench as bench
import k8dbbench.cnpg as cnpg

runners = {
    cluster = bench.Runner {
        name = "cluster"
        namespace = "stress-test"
        imagePullPolicy = "IfNotPresent"
        spec.template.spec = {
            serviceAccountName = "stresstester"
            resources.limit = { cpu = "1000m" }
        }
    }
}

benchmarks = {
    loadtest = bench.Benchmark {
        name = "local - mini stress"
        runner = bench.runnerRef(runners.cluster)
        config = bench.K8StressConfig {
            # Each user manages multiple instances and runs their own set of tests.
            users = [
                {
                    name = "loadtester-1"
                    min_postgres: 1
                    max_postgres: 1 # 5
                    updates_interval: { # 60s +/- 30s
                        duration: "60s"
                        jitter: "30s"
                    }
                    rampup: {
                        create_interval: {
                            duration: "5s"
                            jitter: "1s"
                        },
                        min_instances: 1
                        max_instances: 1 # 2
                    },

                    # configs provides a list of possible postgres instance
                    # configurations and TPC-C test configurations. The benchmark
                    # will randomly choose a configuration to start or stop.
                    configs: [
                        {
                            min_lifetime: "5m"
                            instance: {
                                name: "pg-mini-stress"
                                spec: cnpg.Cluster{
                                  resources = { requests = { memory = "100Mi", cpu = "50m" } }
                                  storage.size = "100Mi"
                                  enableSuperuserAccess = True
                                },
                            }
                            test: {
                                live_stats_period = "10s"
                                live_min_age = "20s"
                                warehouses = 1
                                active_terminals = 10
                                count = 10
                                wait_thinking = True
                            }
                        }
                    ]
                },
            ]
        }
    }
}
