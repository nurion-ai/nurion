"""Aether service deployment for nightly E2E testing.

Deploys:
- PostgreSQL StatefulSet for database
- Aether FastAPI service Deployment
- ConfigMaps and Secrets
- Service for API access
"""

from typing import Optional

import pulumi
import pulumi_kubernetes as k8s
import pulumi_random as random

from config import InfraConfig, get_common_labels


def deploy_postgresql(
    config: InfraConfig,
    namespace: k8s.core.v1.Namespace,
    k8s_provider: k8s.Provider,
) -> dict:
    """Deploy PostgreSQL for Aether database.
    
    Args:
        config: Infrastructure configuration
        namespace: Kubernetes namespace
        k8s_provider: Kubernetes provider
        
    Returns:
        Dict with PostgreSQL resources and connection info
    """
    labels = get_common_labels("postgresql")
    labels["app"] = "postgresql"
    
    # Secret for PostgreSQL credentials
    postgres_secret = k8s.core.v1.Secret(
        "postgresql-secret",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="postgresql-secret",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        type="Opaque",
        string_data={
            "POSTGRES_USER": "aether",
            "POSTGRES_PASSWORD": config.postgres_password,
            "POSTGRES_DB": "aether",
        },
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # PVC for PostgreSQL data
    postgres_pvc = k8s.core.v1.PersistentVolumeClaim(
        "postgresql-data",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="postgresql-data",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        spec=k8s.core.v1.PersistentVolumeClaimSpecArgs(
            access_modes=["ReadWriteOnce"],
            resources=k8s.core.v1.VolumeResourceRequirementsArgs(
                requests={"storage": config.postgres_storage_size},
            ),
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # PostgreSQL StatefulSet
    postgres_statefulset = k8s.apps.v1.StatefulSet(
        "postgresql",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="postgresql",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        spec=k8s.apps.v1.StatefulSetSpecArgs(
            service_name="postgresql",
            replicas=1,
            selector=k8s.meta.v1.LabelSelectorArgs(
                match_labels={"app": "postgresql"},
            ),
            template=k8s.core.v1.PodTemplateSpecArgs(
                metadata=k8s.meta.v1.ObjectMetaArgs(
                    labels={"app": "postgresql"},
                ),
                spec=k8s.core.v1.PodSpecArgs(
                    containers=[
                        k8s.core.v1.ContainerArgs(
                            name="postgresql",
                            image="postgres:16-alpine",
                            ports=[
                                k8s.core.v1.ContainerPortArgs(
                                    container_port=5432,
                                    name="postgres",
                                ),
                            ],
                            env_from=[
                                k8s.core.v1.EnvFromSourceArgs(
                                    secret_ref=k8s.core.v1.SecretEnvSourceArgs(
                                        name=postgres_secret.metadata.name,
                                    ),
                                ),
                            ],
                            resources=k8s.core.v1.ResourceRequirementsArgs(
                                requests={"cpu": "250m", "memory": "256Mi"},
                                limits={"cpu": "1", "memory": "1Gi"},
                            ),
                            volume_mounts=[
                                k8s.core.v1.VolumeMountArgs(
                                    name="data",
                                    mount_path="/var/lib/postgresql/data",
                                ),
                            ],
                            liveness_probe=k8s.core.v1.ProbeArgs(
                                exec_=k8s.core.v1.ExecActionArgs(
                                    command=["pg_isready", "-U", "aether"],
                                ),
                                initial_delay_seconds=30,
                                period_seconds=10,
                            ),
                            readiness_probe=k8s.core.v1.ProbeArgs(
                                exec_=k8s.core.v1.ExecActionArgs(
                                    command=["pg_isready", "-U", "aether"],
                                ),
                                initial_delay_seconds=5,
                                period_seconds=5,
                            ),
                        ),
                    ],
                    volumes=[
                        k8s.core.v1.VolumeArgs(
                            name="data",
                            persistent_volume_claim=k8s.core.v1.PersistentVolumeClaimVolumeSourceArgs(
                                claim_name=postgres_pvc.metadata.name,
                            ),
                        ),
                    ],
                ),
            ),
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # PostgreSQL Service
    postgres_service = k8s.core.v1.Service(
        "postgresql",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="postgresql",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        spec=k8s.core.v1.ServiceSpecArgs(
            selector={"app": "postgresql"},
            ports=[
                k8s.core.v1.ServicePortArgs(
                    port=5432,
                    target_port=5432,
                    name="postgres",
                ),
            ],
            cluster_ip="None",  # Headless service for StatefulSet
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Connection string for Aether
    db_url = pulumi.Output.concat(
        "postgresql://aether:",
        config.postgres_password,
        "@postgresql.",
        namespace.metadata.name,
        ".svc.cluster.local:5432/aether"
    )
    
    return {
        "secret": postgres_secret,
        "pvc": postgres_pvc,
        "statefulset": postgres_statefulset,
        "service": postgres_service,
        "connection_url": db_url,
    }


def deploy_aether(
    config: InfraConfig,
    namespace: k8s.core.v1.Namespace,
    k8s_provider: k8s.Provider,
    db_url: pulumi.Output[str],
) -> dict:
    """Deploy Aether FastAPI service.
    
    Args:
        config: Infrastructure configuration
        namespace: Kubernetes namespace
        k8s_provider: Kubernetes provider
        db_url: Database connection URL
        
    Returns:
        Dict with Aether resources
    """
    labels = get_common_labels("aether")
    labels["app"] = "aether"
    
    # ConfigMap for non-secret configuration
    aether_configmap = k8s.core.v1.ConfigMap(
        "aether-config",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="aether-config",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        data={
            "AETHER_ENV": "nightly",
            "AETHER_LOG_LEVEL": "INFO",
            "AETHER_WORKERS": "4",
        },
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Secret for sensitive configuration
    aether_secret = k8s.core.v1.Secret(
        "aether-secret",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="aether-secret",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        type="Opaque",
        string_data={
            "DATABASE_URL": db_url,
            # Add S3 credentials if needed
            "AWS_ACCESS_KEY_ID": config.s3_access_key or "",
            "AWS_SECRET_ACCESS_KEY": config.s3_secret_key or "",
            "AWS_ENDPOINT_URL": f"https://{config.s3_endpoint}",
        },
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Container registry secret for pulling images
    registry_secret = None
    if config.registry_username and config.registry_password:
        import base64
        import json
        
        docker_config = {
            "auths": {
                config.registry_url: {
                    "username": config.registry_username,
                    "password": config.registry_password,
                }
            }
        }
        
        registry_secret = k8s.core.v1.Secret(
            "registry-secret",
            metadata=k8s.meta.v1.ObjectMetaArgs(
                name="registry-secret",
                namespace=namespace.metadata.name,
                labels=labels,
            ),
            type="kubernetes.io/dockerconfigjson",
            string_data={
                ".dockerconfigjson": json.dumps(docker_config),
            },
            opts=pulumi.ResourceOptions(provider=k8s_provider),
        )
    
    # Aether Deployment
    aether_image = f"{config.registry_url}/aether:{config.aether_image_tag}"
    
    image_pull_secrets = []
    if registry_secret:
        image_pull_secrets.append(
            k8s.core.v1.LocalObjectReferenceArgs(name=registry_secret.metadata.name)
        )
    
    aether_deployment = k8s.apps.v1.Deployment(
        "aether",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="aether",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        spec=k8s.apps.v1.DeploymentSpecArgs(
            replicas=config.aether_replicas,
            selector=k8s.meta.v1.LabelSelectorArgs(
                match_labels={"app": "aether"},
            ),
            template=k8s.core.v1.PodTemplateSpecArgs(
                metadata=k8s.meta.v1.ObjectMetaArgs(
                    labels={"app": "aether"},
                ),
                spec=k8s.core.v1.PodSpecArgs(
                    image_pull_secrets=image_pull_secrets if image_pull_secrets else None,
                    init_containers=[
                        # Run database migrations
                        k8s.core.v1.ContainerArgs(
                            name="migrate",
                            image=aether_image,
                            command=["alembic", "upgrade", "head"],
                            env_from=[
                                k8s.core.v1.EnvFromSourceArgs(
                                    config_map_ref=k8s.core.v1.ConfigMapEnvSourceArgs(
                                        name=aether_configmap.metadata.name,
                                    ),
                                ),
                                k8s.core.v1.EnvFromSourceArgs(
                                    secret_ref=k8s.core.v1.SecretEnvSourceArgs(
                                        name=aether_secret.metadata.name,
                                    ),
                                ),
                            ],
                        ),
                    ],
                    containers=[
                        k8s.core.v1.ContainerArgs(
                            name="aether",
                            image=aether_image,
                            ports=[
                                k8s.core.v1.ContainerPortArgs(
                                    container_port=8000,
                                    name="http",
                                ),
                            ],
                            env_from=[
                                k8s.core.v1.EnvFromSourceArgs(
                                    config_map_ref=k8s.core.v1.ConfigMapEnvSourceArgs(
                                        name=aether_configmap.metadata.name,
                                    ),
                                ),
                                k8s.core.v1.EnvFromSourceArgs(
                                    secret_ref=k8s.core.v1.SecretEnvSourceArgs(
                                        name=aether_secret.metadata.name,
                                    ),
                                ),
                            ],
                            resources=k8s.core.v1.ResourceRequirementsArgs(
                                requests={"cpu": "250m", "memory": "512Mi"},
                                limits={"cpu": "1", "memory": "2Gi"},
                            ),
                            liveness_probe=k8s.core.v1.ProbeArgs(
                                http_get=k8s.core.v1.HTTPGetActionArgs(
                                    path="/api/health",
                                    port=8000,
                                ),
                                initial_delay_seconds=30,
                                period_seconds=10,
                            ),
                            readiness_probe=k8s.core.v1.ProbeArgs(
                                http_get=k8s.core.v1.HTTPGetActionArgs(
                                    path="/api/health",
                                    port=8000,
                                ),
                                initial_delay_seconds=5,
                                period_seconds=5,
                            ),
                        ),
                    ],
                ),
            ),
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Aether Service
    aether_service = k8s.core.v1.Service(
        "aether",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="aether",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        spec=k8s.core.v1.ServiceSpecArgs(
            selector={"app": "aether"},
            ports=[
                k8s.core.v1.ServicePortArgs(
                    port=8000,
                    target_port=8000,
                    name="http",
                ),
            ],
            type="ClusterIP",
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Internal URL for tests
    aether_url = pulumi.Output.concat(
        "http://aether.",
        namespace.metadata.name,
        ".svc.cluster.local:8000"
    )
    
    return {
        "configmap": aether_configmap,
        "secret": aether_secret,
        "registry_secret": registry_secret,
        "deployment": aether_deployment,
        "service": aether_service,
        "url": aether_url,
    }


def deploy_aether_stack(
    config: InfraConfig,
    namespace: k8s.core.v1.Namespace,
    k8s_provider: k8s.Provider,
) -> dict:
    """Deploy complete Aether stack (PostgreSQL + Aether service).
    
    Args:
        config: Infrastructure configuration
        namespace: Kubernetes namespace
        k8s_provider: Kubernetes provider
        
    Returns:
        Dict with all deployed resources
    """
    # Deploy PostgreSQL first
    postgres = deploy_postgresql(config, namespace, k8s_provider)
    
    # Deploy Aether with database connection
    aether = deploy_aether(
        config,
        namespace,
        k8s_provider,
        postgres["connection_url"],
    )
    
    return {
        "postgres": postgres,
        "aether": aether,
        "aether_url": aether["url"],
    }
