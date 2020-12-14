headId=$(head -1 /home/admin/constellation/whitelisting | cut -d "," -f 1);\
  curl -v -s -X POST http://127.0.0.1:9002/join -H 'Content-type: application/json' -d "{ \"host\": \"$headIP\", \"port\": 9001, \"id\": \"$headId\" }"
