"""GitHub Actions Runner Controller (ARC) deployment for self-hosted runners.

Uses actions-runner-controller v2 with RunnerScaleSet for autoscaling.
See: https://github.com/actions/actions-runner-controller
"""

from typing import List, Optional

import pulumi
import pulumi_kubernetes as k8s

from config import InfraConfig, get_common_labels


def deploy_actions_runner_controller(
    config: InfraConfig,
    namespace: k8s.core.v1.Namespace,
    k8s_provider: k8s.Provider,
) -> dict:
    """Deploy GitHub Actions Runner Controller with RunnerScaleSet.
    
    Args:
        config: Infrastructure configuration
        namespace: Kubernetes namespace for deployment
        k8s_provider: Kubernetes provider
        
    Returns:
        Dict with deployed resources
    """
    labels = get_common_labels("runner")
    
    # Create namespace for ARC controller (separate from workload namespace)
    arc_system_ns = k8s.core.v1.Namespace(
        "arc-system",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="arc-system",
            labels=labels,
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Deploy ARC controller using Helm
    # Use traditional Helm repo instead of OCI registry for better compatibility
    # Alternative repo: https://danmanners.github.io/gha-scale-set-helm
    arc_controller = k8s.helm.v3.Release(
        "arc-controller",
        chart="gha-runner-scale-set-controller",
        repository_opts=k8s.helm.v3.RepositoryOptsArgs(
            repo="https://danmanners.github.io/gha-scale-set-helm",
        ),
        namespace=arc_system_ns.metadata.name,
        values={
            "replicaCount": 1,
            "image": {
                # Use a China-accessible mirror if needed
                "repository": "ghcr.io/actions/gha-runner-scale-set-controller",
                "tag": "0.9.3",
            },
        },
        opts=pulumi.ResourceOptions(
            provider=k8s_provider,
            depends_on=[arc_system_ns],
        ),
    )
    
    # Create secret for GitHub App or PAT authentication
    github_auth_secret = k8s.core.v1.Secret(
        "github-auth-secret",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="github-auth-secret",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        type="Opaque",
        string_data={
            "github_token": config.github_token,
        },
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Deploy RunnerScaleSet for the nurion repository
    # Use traditional Helm repo instead of OCI registry for better compatibility
    runner_scale_set = k8s.helm.v3.Release(
        "nurion-runners",
        chart="gha-runner-scale-set",
        repository_opts=k8s.helm.v3.RepositoryOptsArgs(
            repo="https://danmanners.github.io/gha-scale-set-helm",
        ),
        namespace=namespace.metadata.name,
        values={
            "runnerScaleSetName": "nurion-sh-runners",
            "githubConfigUrl": f"https://github.com/{config.github_repo}",
            "githubConfigSecret": github_auth_secret.metadata.name,
            "minRunners": config.runner_replicas_min,
            "maxRunners": config.runner_replicas_max,
            "runnerGroup": "default",
            "containerMode": {
                "type": "dind",  # Docker-in-Docker for container builds
            },
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "runner",
                            "image": "ghcr.io/actions/actions-runner:latest",
                            "resources": {
                                "requests": {
                                    "cpu": "2",
                                    "memory": "4Gi",
                                },
                                "limits": {
                                    "cpu": "4",
                                    "memory": "8Gi",
                                },
                            },
                            "env": [
                                # China mirror environment variables
                                {
                                    "name": "PIP_INDEX_URL",
                                    "value": "https://mirrors.aliyun.com/pypi/simple/",
                                },
                                {
                                    "name": "PIP_TRUSTED_HOST",
                                    "value": "mirrors.aliyun.com",
                                },
                            ],
                            "volumeMounts": [
                                {
                                    "name": "work",
                                    "mountPath": "/home/runner/_work",
                                },
                                {
                                    "name": "tool-cache",
                                    "mountPath": "/opt/hostedtoolcache",
                                },
                            ],
                        },
                    ],
                    "volumes": [
                        {
                            "name": "work",
                            "emptyDir": {},
                        },
                        {
                            "name": "tool-cache",
                            "persistentVolumeClaim": {
                                "claimName": "runner-tool-cache",
                            },
                        },
                    ],
                },
            },
            "controllerServiceAccount": {
                "namespace": arc_system_ns.metadata.name,
                "name": "arc-controller-gha-runner-scale-set-controller",
            },
        },
        opts=pulumi.ResourceOptions(
            provider=k8s_provider,
            depends_on=[arc_controller, github_auth_secret],
        ),
    )
    
    # Create PVC for tool cache (Maven, pip packages, etc.)
    tool_cache_pvc = k8s.core.v1.PersistentVolumeClaim(
        "runner-tool-cache",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="runner-tool-cache",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        spec=k8s.core.v1.PersistentVolumeClaimSpecArgs(
            access_modes=["ReadWriteMany"],
            resources=k8s.core.v1.VolumeResourceRequirementsArgs(
                requests={"storage": "50Gi"},
            ),
            # Use default storage class or specify Volcengine storage class
            # storage_class_name="volcengine-nas",
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    return {
        "arc_system_namespace": arc_system_ns,
        "arc_controller": arc_controller,
        "runner_scale_set": runner_scale_set,
        "github_auth_secret": github_auth_secret,
        "tool_cache_pvc": tool_cache_pvc,
    }


def deploy_runner_simple(
    config: InfraConfig,
    namespace: k8s.core.v1.Namespace,
    k8s_provider: k8s.Provider,
) -> dict:
    """Deploy a simple self-hosted runner as a Deployment (fallback option).
    
    Use this if ARC is not available or for simpler setups.
    """
    labels = get_common_labels("runner")
    labels["app"] = "github-runner"
    
    # Create secret for GitHub token
    github_secret = k8s.core.v1.Secret(
        "github-runner-secret",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="github-runner-secret",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        type="Opaque",
        string_data={
            "RUNNER_TOKEN": config.github_token,
        },
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Runner deployment
    runner_deployment = k8s.apps.v1.Deployment(
        "github-runner",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name="github-runner",
            namespace=namespace.metadata.name,
            labels=labels,
        ),
        spec=k8s.apps.v1.DeploymentSpecArgs(
            replicas=config.runner_replicas_min,
            selector=k8s.meta.v1.LabelSelectorArgs(
                match_labels={"app": "github-runner"},
            ),
            template=k8s.core.v1.PodTemplateSpecArgs(
                metadata=k8s.meta.v1.ObjectMetaArgs(
                    labels={"app": "github-runner"},
                ),
                spec=k8s.core.v1.PodSpecArgs(
                    containers=[
                        k8s.core.v1.ContainerArgs(
                            name="runner",
                            image="myoung34/github-runner:latest",
                            env=[
                                k8s.core.v1.EnvVarArgs(
                                    name="REPO_URL",
                                    value=f"https://github.com/{config.github_repo}",
                                ),
                                k8s.core.v1.EnvVarArgs(
                                    name="RUNNER_NAME_PREFIX",
                                    value="nurion-sh",
                                ),
                                k8s.core.v1.EnvVarArgs(
                                    name="RUNNER_WORKDIR",
                                    value="/home/runner/_work",
                                ),
                                k8s.core.v1.EnvVarArgs(
                                    name="LABELS",
                                    value=",".join(config.runner_labels),
                                ),
                                k8s.core.v1.EnvVarArgs(
                                    name="ACCESS_TOKEN",
                                    value_from=k8s.core.v1.EnvVarSourceArgs(
                                        secret_key_ref=k8s.core.v1.SecretKeySelectorArgs(
                                            name=github_secret.metadata.name,
                                            key="RUNNER_TOKEN",
                                        ),
                                    ),
                                ),
                                # China mirrors
                                k8s.core.v1.EnvVarArgs(
                                    name="PIP_INDEX_URL",
                                    value="https://mirrors.aliyun.com/pypi/simple/",
                                ),
                            ],
                            resources=k8s.core.v1.ResourceRequirementsArgs(
                                requests={"cpu": "2", "memory": "4Gi"},
                                limits={"cpu": "4", "memory": "8Gi"},
                            ),
                            volume_mounts=[
                                k8s.core.v1.VolumeMountArgs(
                                    name="docker-sock",
                                    mount_path="/var/run/docker.sock",
                                ),
                            ],
                        ),
                    ],
                    volumes=[
                        k8s.core.v1.VolumeArgs(
                            name="docker-sock",
                            host_path=k8s.core.v1.HostPathVolumeSourceArgs(
                                path="/var/run/docker.sock",
                            ),
                        ),
                    ],
                ),
            ),
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    return {
        "github_secret": github_secret,
        "runner_deployment": runner_deployment,
    }
