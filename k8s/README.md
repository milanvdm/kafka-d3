# Minikube

## Setup

```
minikube config set cpus 4
minikube config set memory 8192

minikube start

helm init --upgrade
helm repo add confluentinc https://raw.githubusercontent.com/confluentinc/cp-helm-charts/master

helm install -f k8s/kafka-values.yaml --set cp-kafka-connect.enabled=false confluentinc/cp-helm-charts --name kafka
helm install stable/prometheus --name promotheus
helm install stable/grafana --name grafana

kubectl exec -ti kafka-d3:0.1.0 -- sbt

helm delete kafka
```

## Monitoring

```
minikube addons enable heapster
minikube addons open heapster
```