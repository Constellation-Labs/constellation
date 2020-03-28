#!/usr/bin/env sh

URL="${url}"

QUERY='
  split("\n")
  | map(split(","))
  | map(select(. != []))
  | map({
    targets: [(.[0]+":9000")],
    labels: {
      alias: "constellation",
      id: .[1]
    }
  })
'

RESPONSE=$(wget -qO- $URL) && \
JSON=$(printf "$RESPONSE" | jq -Rs '.' | jq "$QUERY") && \
printf "$JSON" | jq '.' > /home/admin/grafana-dashboard/prometheus/targets/targets.json