#!/usr/bin/env bash
(crontab -l 2>/dev/null; echo "* * * * * cd ~/grafana-dashboard/prometheus && ./cluster_info_to_targets.sh") | crontab - && \
echo "Node discovery cron setup succeeded"