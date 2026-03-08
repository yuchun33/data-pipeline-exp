# data pipeline exp

## infra architecture

- deployment: python
- daemonSet: prometheus
- statefulSet: miniO
- igness:
- operator:

## dashboard preview

operation

## medallion lakehouse

- date-gen: kafka -> kafka connect -> miniO
- bronze: merge daily logs into parquet. | kafka -> airflow/daily_bronze_merge_production_log.py -> miniO
- silver: handle ETL and add readable info. | miniO -> airflow/daily_silver_etl.py -> miniO
- gold: static calculation and report. | miniO -> airflow/daily_golden_gen_table.py -> minio/sql -> realtime dashboard

## spark verify

## tools

- [x] python
- [ ] angular
- [x] kafka
- [x] kafka-connect
- [ ] nginx
- [ ] argoCD
- [ ] elasticsearch
- [ ] sql
- [x] airflow
- [ ] spark
- [x] miniO(minioadmin/minioadmin)
- [x] prometheus & grafana
- [x] kubernetes
- [x] helm
- [x] docker
- [x] github action

- [x] azure vm

## robust

- autoscaler(HPA)
- namespace isolation
  - infra: kafka, miniO, ES, SQL, argoCD
  - processing: airflow, spark
  - apps: angular, python, nginx
  - monitoring: prometheus
- requests & limits of resources & quotas
- liveness & readiness probe
- node affinity & anti-affinity(node pool)

### kafka-connect

1. 使用 kafka-connect.yaml 建立 kafka-connect
2. `kubectl port-forward svc/kafka-connect-service 8083:8083`
3. 使用 minio-sink-config.json 建立 connector

```cmd
      curl -X POST http://localhost:8083/connectors \
        -H "Content-Type: application/json" \
        -d minio-sink-config.json
```

### prometheus & grafana

1. 使用 helm 安裝 kube-prometheus-stack
2. 安裝後可依照指示取回登入帳密

```cmd
Get Grafana 'admin' user password by running:

  kubectl --namespace kube-prometheus-stack get secrets kube-prometheus-stack-grafana -o jsonpath="{.data.admin-password}" | base64 -d ; echo

Access Grafana local instance:

  export POD_NAME=$(kubectl --namespace kube-prometheus-stack get pod -l "app.kubernetes.io/name=grafana,app.kubernetes.io/instance=kube-prometheus-stack" -oname)
  kubectl --namespace kube-prometheus-stack port-forward $POD_NAME 3000
```
