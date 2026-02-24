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
- bronze: kafka -> python consumer (airflow dag every 5 mins) -> miniO
- silver: miniO -> (add more info) -> miniO
- gold: miniO -> elasticsearch -> realtime dashboard

tools
- python
- angular
- kafka
- nginx
- argoCD
- elasticsearch
- sql
- airflow
- spark
- miniO
- prometheus
  
- kubernetes
- helm
- docker
- github action

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