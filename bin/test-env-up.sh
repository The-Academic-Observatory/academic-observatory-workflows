#!/usr/bin/env bash
set -euxo pipefail

usage() {
    echo "This script builds and starts the Academic Observatory test environment"
    echo "Usage: $0 [--no-build]"
    echo "       $0 [--help]"
    echo
    echo "--no-build  Will not build the AO workflows Docker image."
    echo "--help      Display this help message"
    exit 1
}

arg="${1:-}"
if [ "$arg" == "--help" ]; then
    usage
elif [ "$arg" != "--no-build" ] && [ -n "$arg" ]; then
    echo "Invalid argument: $arg"
    usage
fi

# Authenticate minikube with gcp
if [ -f .env ]; then # Source the .env file
    source .env
fi
if [ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]; then
    echo "GOOGLE_APPLICATION_CREDENTIALS is not set in '.env'. This is required to run the tests."
    exit 1
fi
export GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS

# Kill anything that's using our ports
# sudo fuser -k 5080/tcp || true
# sudo fuser -k 5021/tcp || true

# Delete and start Minikube
minikube delete --all --purge
minikube start \
    --ports=5080,5021 \
    --extra-config=apiserver.service-node-port-range=30000-30009 \
    --wait=all \
    --wait-timeout=2m0s \
    --force \
    --driver=docker
minikube update-context

echo "=== DEBUG: minikube status ==="
minikube status

echo "=== DEBUG: Minikube kubeconfig contents ==="
cat "$HOME/.kube/config"

# Ensure correct context is used
export KUBECONFIG="$HOME/.kube/config"
kubectl config use-context minikube

echo "=== DEBUG: kubectl current context ==="
kubectl config current-context || echo "No current context set"

echo "=== DEBUG: kubectl config view (minified) ==="
kubectl config view --minify || echo "No config found"

echo "=== DEBUG: kubeconfig file exists? ==="
ls -l "$HOME/.kube/config" || echo "Missing ~/.kube/config"

echo "=== DEBUG: trying to reach cluster ==="
kubectl cluster-info || echo "cluster-info failed"


# Wait until the API is ready
for _ in {1..30}; do
    if kubectl get nodes &>/dev/null; then
        echo "Kubernetes API is ready."
        break
    fi
    echo "Waiting for Kubernetes API..." && sleep 2
done

# Enable addons
minikube addons enable gcp-auth

# Run docker-compose services
docker compose -f test-env-compose.yaml build
docker compose -f test-env-compose.yaml down
docker compose -f test-env-compose.yaml up -d

# Use the minikube Docker daemon
eval "$(minikube docker-env)"
if [ "$1" != "--no-build" ]; then
    docker build --no-cache -t academic-observatory:test .
fi

# (Re)Deploy kubernetes config items
kubectl delete --ignore-not-found -f bin/test-konfig.yaml
kubectl apply -f bin/test-konfig.yaml
kubectl cluster-info dump

echo ""
echo "########################### Minikube cluster running ###########################"
echo "######################### Here are some useful commands ########################"
echo ""
echo "--Stop the deployment--"
echo "bash bin/test-env-down.sh"
echo ""
echo "--Monitor the cluster--"
echo "minikube dashboard"
echo "################################################################################"
