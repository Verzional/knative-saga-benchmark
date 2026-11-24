#!/bin/bash
set -e

echo "=== [Kafka] Creating namespace 'kafka' ==="
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -

echo "=== [Kafka] Installing Strimzi Kafka Operator ==="
kubectl apply -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

echo "=== [Kafka] Waiting for Strimzi Operator deployment to be ready ==="
kubectl wait deployment/strimzi-cluster-operator -n kafka --for=condition=Available --timeout=300s || true

echo "=== [Kafka] Deploying Lightweight Kafka Cluster (single node for Kind benchmark) ==="
cat <<EOF | kubectl apply -n kafka -f -
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaNodePool
metadata:
  name: dual-role
  labels:
    strimzi.io/cluster: saga-kafka
spec:
  replicas: 1
  roles:
    - controller
    - broker
  storage:
    type: ephemeral
---
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: saga-kafka
  annotations:
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: 3.7.0
    metadataVersion: 3.7-IV4
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: external
        port: 9094
        type: nodeport
        tls: false
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
      default.replication.factor: 1
      min.insync.replicas: 1
  entityOperator:
    topicOperator: {}
    userOperator: {}
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: order-events
  labels:
    strimzi.io/cluster: saga-kafka
spec:
  partitions: 3
  replicas: 1
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: payment-events
  labels:
    strimzi.io/cluster: saga-kafka
spec:
  partitions: 3
  replicas: 1
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: inventory-events
  labels:
    strimzi.io/cluster: saga-kafka
spec:
  partitions: 3
  replicas: 1
EOF

echo "=== [Kafka] Kafka KRaft Cluster and Topics defined ==="
