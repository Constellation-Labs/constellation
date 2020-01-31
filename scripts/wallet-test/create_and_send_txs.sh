#!/usr/bin/env bash

CSV_FILE=$1
LOAD_BALANCER_ADDRESS=$2
WORKING_DIR=data

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

USAGE="Proper usage of script:
  $(basename "$0") csv_file load_balancer_address"

if [ -z "${CSV_FILE}" ]; then
  echo -e "${RED}Csv file is unset $NC";
  echo "$USAGE";
  exit 1;
else
  echo -e "${GREEN}Csv file is set to = $CSV_FILE \n $NC";
fi

if [ -z "${LOAD_BALANCER_ADDRESS}" ]; then
  echo -e "${RED}Load balancer address is unset $NC";
  echo "$USAGE";
  exit 1;
else
  echo -e "${GREEN}Load balancer address is set to = $LOAD_BALANCER_ADDRESS $NC";
fi

rm -rf $WORKING_DIR/prev_tx-*
rm -rf $WORKING_DIR/new_tx-*

N=0
ARR=()

IFS=","

while read -r STR
do
        set -- "$STR"

        while [ "$#" -gt 0 ]
        do
                ARR[$N]="$1"
                ((N++))
                shift
        done
done < "$CSV_FILE"

SIZE=${#ARR[@]}

for (( i=0; i<SIZE; i++ ))
do
  address="$(cut -d',' -f1 <<<"${ARR[i]}")"
  echo -e "Address : ${GREEN}${address}${NC}"
  touch $WORKING_DIR/prev_tx-${i}

  java -jar cl-wallet.jar create-transaction --keystore "${WORKING_DIR}/key-${i}.p12" --alias alias --storepass storepass --keypass keypass -d DAG8eaKi2W7ZirqMJBEnKkEUu5kHoL9gvwaT5BQ7 -p "${WORKING_DIR}/prev_tx-${i}" -f "${WORKING_DIR}/new_tx-${i}" --fee 0 --amount 1
  echo -e "${GREEN}Transaction created for address : ${address} ${NC}"

  url=http://"${LOAD_BALANCER_ADDRESS}":9000/transaction
  path=${WORKING_DIR}/new_tx-${i}
  curl -X POST "$url" -H "Content-type: application/json" --data-binary @"${path}"
  echo -e "${GREEN}Transaction created for address : ${address} sent to ${LOAD_BALANCER_ADDRESS} ${NC}"
done
