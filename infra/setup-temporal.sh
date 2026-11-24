#!/bin/bash
set -e

echo "=== [Temporal] Creating namespace 'temporal' ==="
kubectl create namespace temporal --dry-run=client -o yaml | kubectl apply -f -

echo "=== [Temporal] Deploying PostgreSQL backend ==="
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: temporal
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:15-alpine
        args: ["-c", "max_connections=500", "-c", "shared_buffers=512MB"]
        env:
        - name: POSTGRES_USER
          value: temporal
        - name: POSTGRES_PASSWORD
          value: temporal
        - name: POSTGRES_DB
          value: temporal
        ports:
        - containerPort: 5432
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: temporal
spec:
  ports:
  - port: 5432
  selector:
    app: postgres
EOF

echo "=== [Temporal] Deploying Temporal Server standalone for Kind ==="
cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: temporal-server
  namespace: temporal
spec:
  replicas: 1
  selector:
    matchLabels:
      app: temporal-server
  template:
    metadata:
      labels:
        app: temporal-server
    spec:
      containers:
      - name: temporal-server
        image: temporalio/auto-setup:1.24.0
        env:
        - name: DB
          value: postgres12
        - name: DB_PORT
          value: "5432"
        - name: POSTGRES_USER
          value: temporal
        - name: POSTGRES_PWD
          value: temporal
        - name: POSTGRES_SEEDS
          value: postgres.temporal.svc.cluster.local
        - name: SQL_MAX_CONNS
          value: "200"
        - name: SQL_MAX_IDLE_CONNS
          value: "50"
        ports:
        - containerPort: 7233
          name: rpc
        - containerPort: 9090
          name: metrics
---
apiVersion: v1
kind: Service
metadata:
  name: temporal-frontend
  namespace: temporal
spec:
  ports:
  - port: 7233
    targetPort: 7233
    name: rpc
  - port: 9090
    targetPort: 9090
    name: metrics
  selector:
    app: temporal-server
EOF

echo "=== [Temporal] Temporal server manifests applied ==="
