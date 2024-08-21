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

# Check if minikube is running, if not, start it
minikube status || minikube start --network=bridge --driver=docker

# Authenticate minikube with gcp
if [ -f .env ]; then # Source the .env file
  source .env
else
  echo ".env file not found."
  exit 1
fi
if [ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]; then
  echo "GOOGLE_APPLICATION_CREDENTIALS is not set in '.env'. This is required to run the tests."
  exit 1
fi
export GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS
minikube addons enable gcp-auth

# Use the minikube docker daemon to build Academic Observatory workflows image
eval $(minikube docker-env)
docker build -t ao-tests-flask-app -f flask-Dockerfile .
if [ "$1" != "--no-build" ]; then
    docker build --no-cache -t academic-observatory:test .
fi

# (Re)Deploy kubernetes config items
kubectl delete --ignore-not-found -f test-konfig.yaml
kubectl apply -f test-konfig.yaml

echo ""
echo "########################### Minikube cluster running ###########################"
echo "######################### Here are some useful commands ########################"
echo ""
echo "--Stop the deployment--"
echo "minikube stop"
echo ""
echo "--Monitor the cluster--"
echo "minikube dashboard"
echo ""
echo "--Debug flask routing--"
echo "kubectl port-forward svc/flask-app-service 5000:80"
echo ""
echo "################################################################################"
