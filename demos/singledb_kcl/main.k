import k8dbbench as bench

systems = {
  local = bench.CNPGSystem {
    name: "local"
    namespace: "benchmark"
    spec.storage.size: "5Gi"
  }
}

runners = {
  local = bench.Runner {
    name: "local"
    namespace: "benchmark"
    systems: bench.systemRefList([systems.local])
    imagePullPolicy: "IfNotPresent"  # We assume the image is built within the cluster
    spec: {
      template.spec: {
        resources.requests.cpu: "100m"
      }
    }
  }
}

make_tpcc_benchmark = lambda runner:bench.Runner, name:str, sf:int {
  bench.TPCCBenchmark {
    name: name
    runner: bench.runnerRef(runner)
    config = {
      warehouses = sf
      active_terminals = sf * 10
      duration = "1m"
      wait_thinking = False
    }
  }
}

make_tpch_benchmark = lambda runner:bench.Runner, name: str, sf:int {
  bench.TPCHBenchmark {
    name: name
    runner: bench.runnerRef(runner)
    config = {
      scale_factor: 1
      count: 1  # number of runs
      queries: ["q1", "q2", "q3", "q4", "q5", "q6", "q7"]  # just a few queries
      enable_query_tuning: True
    }
  }
}

make_benchmarks = lambda runner:bench.Runner {
  {
    name: make_tpcc_benchmark(runner, name, sf)
    for name, sf  in {
      "${runner.name}-tpcc-${sf}": sf for sf in [1, 2, 5, 10]
    }
  } | {
    name: make_tpch_benchmark(runner, name, sf)
    for name, sf  in {
      "${runner.name}-tpch-${sf}": sf for sf in [1, 2, 5]
    }
  }
}

benchmarks = make_benchmarks(runners.local)
