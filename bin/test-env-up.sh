#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "This script builds and starts the Academic Observatory test environment"
    echo "Usage: $0 [--no-build]"
    echo "       $0 [--remote]"
    echo "       $0 [--help]"
    echo
    echo "--no-build  Will not build the AO workflows Docker image."
    echo "--remote    Will run as if in Github Actions"
    echo "--help      Display this help message"
    exit 1
}


get_args() {
    local OPTIONS
    OPTIONS=$(getopt -o h --long help,no-build,remote -- "$@")
    if [ $? -ne 0 ]; then
        usage
        exit 1
    fi

    eval set -- "$OPTIONS"
    while true; do
        case "$1" in
            -h|--help)
                usage
                ;;
            --no-build)
                nobuild=true
                shift
                ;;
            --remote)
                remote=true
                shift
                ;;
            --)
                shift
                break
                ;;
            *)
                echo "Unexpected option: $1"
                usage
                ;;
        esac
    done
}

nobuild=false
remote=false
get_args "$@"

# Authenticate minikube with gcp
if [ -f .env ]; then # Source the .env file
    source .env
fi
if [ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]; then
    echo "GOOGLE_APPLICATION_CREDENTIALS is not set in '.env'. This is required to run the tests."
    exit 1
fi

# Delete and start Minikube
if [ "${remote}" = "false" ]; then
    minikube delete --all --purge
    minikube start \
        --ports=5080,5021 \
        --extra-config=apiserver.service-node-port-range=30000-30009 \
        --wait=all \
        --wait-timeout=2m0s \
        --force \
        --driver=docker
    minikube update-context
fi

# Wait until the API is ready
for _ in {1..30}; do
    if kubectl get nodes &>/dev/null; then
        echo "Kubernetes API is ready."
        break
    fi
    echo "Waiting for Kubernetes API..." && sleep 2
done

# Ensure correct context is used
export KUBECONFIG="$HOME/.kube/config"

if [ "${remote}" = "false" ]; then
    # Enable addons
    minikube addons enable gcp-auth
fi

# Run the compose commands to spin up the servers
docker compose -f test-env-compose.yaml build
docker compose -f test-env-compose.yaml down
docker compose -f test-env-compose.yaml up -d

# Use the minikube Docker daemon
eval "$(minikube docker-env)"
if [ "${nobuild}" = "false" ]; then
    docker build --no-cache -t academic-observatory:test .
fi

# (Re)Deploy kubernetes config items
kubectl delete --ignore-not-found -f bin/test-konfig.yaml
kubectl apply -f bin/test-konfig.yaml
kubectl cluster-info

echo ""
echo "########################### Minikube cluster running ###########################"
echo "######################### Here are some useful commands ########################"
echo ""
echo "--Stop the deployment--"
echo "bash bin/test-env-down.sh"
echo ""
echo "--Monitor the cluster--"
echo "minikube dashboard"
echo ""
echo "################################################################################"
