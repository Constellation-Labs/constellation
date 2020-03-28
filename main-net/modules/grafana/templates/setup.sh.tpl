if ${discovery_mode};
then
  /home/admin/grafana-dashboard/prometheus/cluster_info_to_targets.sh && \
  /home/admin/grafana-dashboard/prometheus/setup_target_discovery_cron.sh && \
  echo "Started Grafana in discovery mode."
else
  /home/admin/grafana-dashboard/prometheus/whitelisting_to_targets.sh && \
  echo "Started Grafana with whitelisting file."
fi