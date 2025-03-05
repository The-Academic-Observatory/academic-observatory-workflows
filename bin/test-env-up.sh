#!/usr/bin/env bash
set -e

usage() {
    echo "This script builds and starts the Academic Observatory test environment"
    echo "Usage: $0 [--no-build]"
    echo "       $0 [--help]"
    echo
    echo "--no-build  Will not build the AO workflows Docker image."
    echo "--help      Display this help message"
    exit 1
}
if [ "$1" == "--help" ]; then
    usage
elif [ "$1" != "--no-build" ] && [ "$1" != "--help" ] && [ ! -z "$1" ]; then
    echo "Invalid argument: $1"
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

# Check if minikube is running, if not, start it
minikube delete --all --purge
minikube start --ports=5080,5021 --extra-config=apiserver.service-node-port-range=30000-30009 --network=bridge --wait-timeout=2m0s --force
minikube addons enable gcp-auth
echo $(minikube ssh grep host.minikube.internal /etc/hosts | cut -f1) host.minikube.internal >> /etc/hosts

# Run the compose commands to spin up the servers
docker compose -f test-env-compose.yaml build
docker compose -f test-env-compose.yaml down
docker compose -f test-env-compose.yaml up -d

# Use the minikube docker daemon to build Academic Observatory workflows image
eval $(minikube docker-env)
if [ "$1" != "--no-build" ]; then
    docker build --no-cache -t academic-observatory:test .
fi

# (Re)Deploy kubernetes config items
kubectl delete --ignore-not-found -f bin/test-konfig.yaml
kubectl apply -f bin/test-konfig.yaml

# # Unset the minikube docker environment
# eval $(minikube docker-env --unset)

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
