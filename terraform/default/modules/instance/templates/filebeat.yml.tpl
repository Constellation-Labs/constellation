filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /var/log/syslog
output.elasticsearch:
  hosts: ["${es_ip}"]
setup.kibana:
  host: "${es_ip}:5601"