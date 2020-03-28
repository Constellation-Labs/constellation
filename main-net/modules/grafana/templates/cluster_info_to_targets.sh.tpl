#!/usr/bin/env sh

URL="${url}"

QUERY='
  map({
    targets: [(.ip.host + ":9000")],
    labels: {
      alias: "constellation",
      id: .id.hex
    }
  })
'

RESPONSE=$(curl $URL) && \
JSON=$(echo "$RESPONSE" | jq '.' | jq "$QUERY") && \
printf "$JSON" | jq '.' > /home/admin/grafana-dashboard/prometheus/targets/targets.json
