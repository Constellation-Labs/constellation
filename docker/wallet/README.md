# constellationprotocol/wallet

## NOT FOR PRODUCTION!
This image contains the outdated full-node and should be used for development purposes only. 

## Run

The working dir is set to `/var/lib/constellation/shared` so to be able to use generated files, one need to mount this volume to the host.

You can run wallet providing class path and cli parameters directly to that command:

```
docker run -v `pwd`/shared:/var/lib/constellation/shared constellationprotocol/wallet:0.0.1
```

There are three objects which can be used:
1. `org.constellation.util.wallet.CreateNewTransaction`
2. `org.constellation.util.wallet.ExportDecryptedKeys`
3. `org.constellation.util.wallet.GenerateAddress`

### GenerateAddress

```
docker run -v `pwd`/shared:/var/lib/constellation/shared constellationprotocol/wallet:0.0.1 \
org.constellation.util.wallet.GenerateAddress --pub_key_str=MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAEwL78JcMnzMHM+bHm2NjlcF6PghRAvZU//Nwn/6O9Ckh6QBApecq3ybAFaOWPRyADy6lIKRRvGw+YxL714+lO1Q== --store_path=address
```

### ExportDecryptedKeys

```
docker run -v `pwd`/shared:/var/lib/constellation/shared constellationprotocol/wallet:0.0.1 \
org.constellation.util.wallet.ExportDecryptedKeys --keystore=test-kp.p12 --alias=alias --storepass=storepass --keypass=keypass --priv_store_path=decrypted_keystore --pub_store_path=decrypted_keystore.pub
```

### CreateNewTransaction

```
docker run -v `pwd`/shared:/var/lib/constellation/shared constellationprotocol/wallet:0.0.1 \
org.constellation.util.wallet.CreateNewTransaction --keystore=test-kp.p12 --alias=alias --storepass=storepass --keypass=keypass --account_path=account --amount=1 --fee=0.007297 --destination=DAG6o9dcxo2QXCuJS8wnrR944YhFBpwc2jsh5j8f --store_path=new-tx --priv_key_str=aaaa --pub_key_str=bbbb
```