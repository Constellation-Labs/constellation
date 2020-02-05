#!/usr/bin/env bash

NUMBER_OF_WALLETS=$1
WORKING_DIR=data
FILENAME=data.csv

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

USAGE="Proper usage of script:
  $(basename "$0") number-of-wallets"

if [ -z "${NUMBER_OF_WALLETS}" ]; then
  echo -e "${RED}Number of wallets is unset $NC";
  echo "$USAGE";
  exit 1;
else
  echo -e "${GREEN}Number of wallets is set to = $NUMBER_OF_WALLETS $NC";
fi

if [ -d "$WORKING_DIR" ]; then
  echo -e "\nRemoving data folder : ${WORKING_DIR}"
  rm -rf $WORKING_DIR;
fi

echo -e "Creating data folder : ${WORKING_DIR} \n"
mkdir -p $WORKING_DIR

for (( i=0; i<NUMBER_OF_WALLETS; i++ ))
do
  java -jar cl-keytool.jar --keystore "data/key-${i}.p12" --alias alias --storepass storepass --keypass keypass
  address=$(java -jar cl-wallet.jar show-address --keystore "data/key-${i}.p12" --alias alias --storepass storepass --keypass keypass)
  echo -e "Created new key with address : $GREEN $address $NC"
  echo "${address},90000000" >> ${WORKING_DIR}/${FILENAME}
done
