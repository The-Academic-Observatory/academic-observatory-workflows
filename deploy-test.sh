#!/usr/bin/env bash
set -e

# This script builds the flask and academic observatory containers and deploys them to the minikube cluster

# Check if minikube is running, if not, start it
minikube status || minikube start

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

# Use the minikube docker daemon
eval $(minikube docker-env)

# Build the Docker images. Use the test tag for academic-observatory 
# docker build -t flask-app -f flask-Dockerfile .
# docker build --no-cache -t academic-observatory:test .

# (Re)Deploy flask deployment and service
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
