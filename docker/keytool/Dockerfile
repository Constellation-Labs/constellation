FROM davidcaste/alpine-java-unlimited-jce

RUN adduser -S -h /var/lib/constellation constellation

COPY cl-keytool.jar /var/lib/constellation/cl-keytool.jar

WORKDIR /var/lib/constellation/

ENV CL_KEYSTORE_NAME ""
ENV CL_ALIAS ""
ENV CL_STOREPASS ""
ENV CL_KEYPASS ""

USER constellation

ENTRYPOINT java -jar cl-keytool.jar --keystore /var/lib/constellation/key/$CL_KEYSTORE_NAME --alias=$CL_ALIAS --storepass=$CL_STOREPASS --keypass=$CL_KEYPASS