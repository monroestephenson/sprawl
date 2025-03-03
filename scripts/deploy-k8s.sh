#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# Build and push Docker image
echo "Building Docker image..."
docker build -t sprawl:latest .

# Check if running on Minikube
if command -v minikube &> /dev/null; then
    echo "Running on Minikube, loading image..."
    minikube image load sprawl:latest
fi

# Create namespace if it doesn't exist
kubectl create namespace sprawl --dry-run=client -o yaml | kubectl apply -f -

# Apply configurations
echo "Applying Kubernetes configurations..."
kubectl apply -f k8s/sprawl-config.yaml -n sprawl
kubectl apply -f k8s/sprawl-statefulset.yaml -n sprawl

# Wait for pods to be ready
echo "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app=sprawl -n sprawl --timeout=300s

# Get service URL
if command -v minikube &> /dev/null; then
    echo -e "${GREEN}Sprawl is available at:${NC}"
    minikube service sprawl-lb -n sprawl --url
else
    echo -e "${GREEN}Sprawl is available at the LoadBalancer external IP:${NC}"
    kubectl get svc sprawl-lb -n sprawl
fi

echo -e "${GREEN}Deployment complete!${NC}"

# Print helpful information
echo -e "\nUseful commands:"
echo "  kubectl get pods -n sprawl"
echo "  kubectl logs -f -l app=sprawl -n sprawl"
echo "  kubectl exec -it sprawl-0 -n sprawl -- /bin/sh" 