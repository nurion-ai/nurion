"""Pulumi infrastructure entry point for Nurion nightly E2E testing.

Deploys:
1. Kubernetes namespace for nightly tests
2. GitHub Actions Runner Controller (ARC) for self-hosted runners
3. Aether service stack (PostgreSQL + FastAPI)

Usage:
    pulumi up -s nightly
    pulumi destroy -s nightly
"""

import pulumi
import pulumi_kubernetes as k8s

from config import load_config, get_common_labels
from runner import deploy_actions_runner_controller, deploy_runner_simple
from aether import deploy_aether_stack


def main():
    """Main entry point for Pulumi program."""
    # Load configuration
    config = load_config()
    
    # Create Kubernetes provider with specific context
    k8s_provider = k8s.Provider(
        "k8s-provider",
        context=config.k8s_context,
    )
    
    # Create namespace for nightly tests
    labels = get_common_labels("namespace")
    
    namespace = k8s.core.v1.Namespace(
        "nurion-nightly",
        metadata=k8s.meta.v1.ObjectMetaArgs(
            name=config.namespace,
            labels=labels,
        ),
        opts=pulumi.ResourceOptions(provider=k8s_provider),
    )
    
    # Deploy GitHub Actions Runner Controller
    # Use ARC for production, or simple runner for simpler setups
    use_arc = pulumi.Config().get_bool("use_arc") or True
    
    if use_arc:
        runner_resources = deploy_actions_runner_controller(
            config=config,
            namespace=namespace,
            k8s_provider=k8s_provider,
        )
    else:
        runner_resources = deploy_runner_simple(
            config=config,
            namespace=namespace,
            k8s_provider=k8s_provider,
        )
    
    # Deploy Aether stack
    aether_resources = deploy_aether_stack(
        config=config,
        namespace=namespace,
        k8s_provider=k8s_provider,
    )
    
    # Export outputs
    pulumi.export("namespace", namespace.metadata.name)
    pulumi.export("aether_url", aether_resources["aether_url"])
    pulumi.export("postgres_service", aether_resources["postgres"]["service"].metadata.name)
    
    # Export runner info
    if use_arc:
        pulumi.export("runner_type", "arc")
        pulumi.export("arc_namespace", runner_resources["arc_system_namespace"].metadata.name)
    else:
        pulumi.export("runner_type", "simple")
        pulumi.export(
            "runner_deployment",
            runner_resources["runner_deployment"].metadata.name
        )


# Run main
main()
