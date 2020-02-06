# TEST TRANSACION SCRIPT

### Generate wallets
```
./create_keys_and_csv.sh NUMBER_OF_KEYS
```

for example:

```
./create_keys_and_csv.sh 10
```


### Create and send transactions
```
./create_and_send_txs.sh CSV_FILE_PATH NODE_IP
```

for example:
```
./create_and_send_txs.sh data/data.csv 35.235.123.147 
```


### Check account balances
```
./print_account_balances.sh CSV_FILE_PATH NODE_IP
```

for example:
```
./print_account_balances.sh data/data.csv 35.235.123.147
```

----

### How to use scripts?

1. Create keys and wallets with a script: `create_keys_and_csv.sh`.
2. Start the cluster with the `data.csv` file generated in the `data` folder.
3. Create transactions and send to the server by script `create_and_send_txs.sh`.
