global:
  scrape_interval: 15s
  scrape_timeout: 10s
  evaluation_interval: 15s
alerting:
  alertmanagers:
  - static_configs:
    - targets: []
    scheme: http
    timeout: 10s
scrape_configs:
- job_name: prometheus
  scrape_interval: 15s
  scrape_timeout: 10s
  metrics_path: /metrics
  scheme: http
  static_configs:
  - targets:
    - localhost:9090
- job_name: constellation
  scrape_interval: 15s
  scrape_timeout: 10s
  metrics_path: /micrometer-metrics
  scheme: http
  static_configs:
  - targets: ${ips_for_grafana}
    labels:
      alias: constellation
