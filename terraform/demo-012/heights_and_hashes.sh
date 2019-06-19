./ips_to_hosts_file.sh | xargs -I {} curl -s http://{}:9000/metrics | jq '.metrics | {hash: .lastSnapshotHash, height: .lastSnapshotHeight, ip: .externalHost}'
