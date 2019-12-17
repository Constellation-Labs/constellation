FROM davidcaste/alpine-java-unlimited-jce

RUN adduser -S -h /var/lib/constellation constellation

COPY keytool.jar /var/lib/constellation/keytool.jar

WORKDIR /var/lib/constellation/

ENV CL_KEYSTORE_NAME ""
ENV CL_ALIAS ""
ENV CL_STOREPASS ""
ENV CL_KEYPASS ""

USER constellation

ENTRYPOINT java -jar keytool.jar --keystore /var/lib/constellation/key/$CL_KEYSTORE_NAME --alias=$CL_ALIAS --storepass=$CL_STOREPASS --keypass=$CL_KEYPASS