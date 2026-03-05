data pipeline


infra architecture
- deployment: python
- daemonSet: prometheus
- statefulSet: miniO
- igness: 
- operator: 

dashboard preview

operation

medallion lakehouse
- bronze: kafka -> kafka connect -> miniO
- silver: miniO -> airflow (add more info) -> miniO
- gold: miniO -> elasticsearch -> realtime dashboard

tools
- [v]python
- angular
- [v]kafka
- [v]kafka-connect
- nginx
- argoCD
- elasticsearch
- sql
- airflow
- spark
- [v]miniO
- [v]prometheus
  
- [v]kubernetes
- [v]helm
- [v]docker
- [v]github action

- azure vm

robust
- autoscaler(HPA)
- namespace isolation
  - infra: kafka, miniO, ES, SQL, argoCD
  - processing: airflow, spark
  - apps: angular, python, nginx
  - monitoring: prometheus
- requests & limits of resources & quotas 
- liveness & readiness probe
- node affinity & anti-affinity(node pool)