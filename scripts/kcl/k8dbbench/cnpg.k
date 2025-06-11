postgresql_version="17"

schema Cluster:
    imageName?: str
    instances: int = 1
    enableSuperuserAccess: bool = False

    inheritedMetadata?: {:}
    affinity?: {:}
    postgresql?: {:}
    monitoring?: {:}
    bootstrap?: {:}
    resources?: {:}
    storage?: {:}
    nodeSelector?: {:}

specs = {
    small = Cluster{
        postgresql.parameters = { max_connections = "500" }
        monitoring.enablePodMonitor = True
        resources = { requests = { memory = "2Gi", cpu = "500m" } }
        storage.size = "1Gi"
    }
}
