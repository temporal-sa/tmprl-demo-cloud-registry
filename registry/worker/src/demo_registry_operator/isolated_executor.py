"""A credential-free Bubblewrap executor profile for Kubernetes components."""
from __future__ import annotations

from typing import Any


FIREWALL_COMMAND = """iptables -F OUTPUT
iptables -P OUTPUT DROP
iptables -A OUTPUT -o lo -j ACCEPT
iptables -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT
iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT
for cidr in 0.0.0.0/8 10.0.0.0/8 100.64.0.0/10 127.0.0.0/8 169.254.0.0/16 172.16.0.0/12 192.0.0.0/24 192.0.2.0/24 192.168.0.0/16 198.18.0.0/15 198.51.100.0/24 203.0.113.0/24 224.0.0.0/4 240.0.0.0/4; do
    iptables -A OUTPUT -d "$cidr" -j DROP
done
iptables -A OUTPUT -p tcp -m multiport --dports 80,443 -j ACCEPT
if ip6tables -L OUTPUT >/dev/null 2>&1; then
    ip6tables -F OUTPUT
    ip6tables -P OUTPUT DROP
    ip6tables -A OUTPUT -o lo -j ACCEPT
    ip6tables -A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
fi
"""


def apply_isolated_executor_profile(
    pod_spec: dict[str, Any], component: dict[str, Any], image: str
) -> None:
    if not component.get("isolatedExecutor"):
        return
    forbidden = [key for key in (
        "worker", "temporalAccess", "temporalProxy", "temporalMtls", "awsIamRoleArn",
        "automountServiceAccountToken", "serverless", "secrets",
    ) if component.get(key)]
    credential_env = [item.get("name", "") for item in component.get("env", [])
                      if item.get("name", "").startswith(("AWS_", "TEMPORAL_"))
                      or item.get("name", "").endswith("API_KEY")]
    if forbidden or credential_env:
        raise ValueError("isolatedExecutor cannot receive runtime credentials: " + ", ".join(forbidden + credential_env))
    pod_spec.update({
        "automountServiceAccountToken": False,
        "securityContext": {
            "runAsNonRoot": True, "runAsUser": 65532, "runAsGroup": 65532,
            "fsGroup": 65532, "fsGroupChangePolicy": "OnRootMismatch",
            "seccompProfile": {"type": "Unconfined"},
        },
        "initContainers": [{
            "name": "executor-firewall", "image": image,
            "command": ["/bin/bash", "-ec", FIREWALL_COMMAND],
            "securityContext": {
                "runAsNonRoot": False, "runAsUser": 0, "runAsGroup": 0,
                "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                "capabilities": {"drop": ["ALL"], "add": ["NET_ADMIN"]},
            },
        }],
    })
    pod_spec.setdefault("volumes", []).extend([
        {"name": "executor-workspaces", "emptyDir": {"sizeLimit": "5Gi"}},
        {"name": "executor-tmp", "emptyDir": {"sizeLimit": "512Mi"}},
    ])
    container = pod_spec["containers"][0]
    container.update({
        "securityContext": {
            "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": [
            {"name": "executor-workspaces", "mountPath": "/workspaces"},
            {"name": "executor-tmp", "mountPath": "/tmp"},
        ],
        "resources": {
            "requests": {"cpu": "250m", "memory": "512Mi"},
            "limits": {"cpu": "2", "memory": "1Gi"},
        },
        "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8082}, "periodSeconds": 5},
        "livenessProbe": {"httpGet": {"path": "/healthz", "port": 8082}, "initialDelaySeconds": 10, "periodSeconds": 15},
    })
