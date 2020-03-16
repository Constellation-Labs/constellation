filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /home/admin/constellation/logs/json_logs/*.log
  json.keys_under_root: true
  json.add_error_key: true
output.elasticsearch:
  hosts: ["${es_ip}"]
setup.kibana:
  host: "${es_ip}:5601"