#!/bin/bash
set -e

echo "=== [Knative] Installing Knative Serving CRDs & Core v1.14 ==="
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.14.0/serving-crds.yaml
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.14.0/serving-core.yaml

echo "=== [Knative] Waiting for Knative Serving control plane pods ==="
kubectl wait --namespace knative-serving \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/name=knative-serving \
  --timeout=180s || true

echo "=== [Knative] Installing Kourier Ingress ==="
kubectl apply -f https://github.com/knative/net-kourier/releases/download/knative-v1.14.0/kourier.yaml

kubectl patch configmap/config-network \
  --namespace knative-serving \
  --type merge \
  --patch '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}'

# Patch Kourier NodePort to match Kind port mapping (31080 -> 80)
kubectl patch service kourier -n kourier-system -p '{"spec":{"type":"NodePort","ports":[{"name":"http2","port":80,"targetPort":8080,"nodePort":31080},{"name":"https","port":443,"targetPort":8443,"nodePort":31443}]}}'

# Configure Knative domain to use nip.io
kubectl patch configmap/config-domain \
  --namespace knative-serving \
  --type merge \
  --patch '{"data":{"192.168.10.10.nip.io":""}}'

# Configure scale-to-zero settings: fast scale-to-zero for cold-start testing
kubectl patch configmap/config-autoscaler \
  --namespace knative-serving \
  --type merge \
  --patch '{"data":{"scale-to-zero-grace-period":"30s","stable-window":"30s"}}'

echo "=== [Knative] Serving and Kourier configured successfully ==="
