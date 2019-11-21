# constellationprotocol/keytool

## Run
To generate a private key you have to provide four environment variables upfront (of course, provided with your values):
```
CL_KEYSTORE_NAME=testkey
CL_ALIAS=alias
CL_STOREPASS=storepass
CL_KEYPASS=keypass
```

The second requirement is to mount your host volume into `/var/lib/constellation/key` where the keytool will generate the key inside the container.

You can run keytool providing env directly or through env file. Below you will find both examples.

```
docker run -e CL_KEYSTORE_NAME=testkey -e CL_ALIAS=alias -e CL_STOREPASS=storepass -e CL_KEYPASS=keypass -v `pwd`/key:/var/lib/constellation/key constellationprotocol/keytool:0.0.1
```

```
docker run --env-file .env -v `pwd`/key:/var/lib/constellation/key constellationprotocol/keytool:0.0.1
```

## Build
To build a docker image one have to provide a current `keytool.jar`. You can compile one using `sbt keytool/assembly` command.
Then, run: `docker build -t constellationprotocol/keytool:0.0.1 -f Dockerfile .` providing the proper version you want to publish.