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
  - trigget period: daily
  - 確認連線 -> 確認 bucket、key 存在 -> 讀取檔案 -> 合併 -> 存合併後的檔案 -> 確認合併前後筆數一致
- silver: handle ETL and add readable info. | miniO -> airflow/daily_silver_etl.py -> miniO
  - trigget period: daily
  - 確認連線 -> 確認 bucket、key、bronze key 存在 -> 讀取檔案 -> 確認預期欄位存在 -> 計算新欄位 -> 判斷欄位合理值 -> 移除不合理值 -> 補齊空值 -> 存檔 -> 確認存檔成功 & 筆數一致
- gold: stat. calculation and report. | miniO -> airflow/daily_golden_gen_table.py -> minio/sql -> realtime dashboard
  - trigger period: once silver folder updated
  - 確認連線 -> 確認 bucket、key、silver key 存在 -> 讀取檔案 -> 統計計算 -> 存檔 -> 通知

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
- [x] airflow http://138.91.2.93:8080/
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
