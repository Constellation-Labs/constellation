ARG BASE_IMAGE=amazoncorretto:8-alpine
FROM $BASE_IMAGE

RUN adduser -S -h /var/lib/constellation constellation

COPY cl-node.jar /var/lib/constellation/cl-node.jar
COPY logback.xml /var/lib/constellation/logback.xml

WORKDIR /var/lib/constellation/

ENV ip "127.0.0.1"
ENV xmx 5G
ENV CL_KEYSTORE_NAME ""
ENV CL_WHITELISTING_NAME ""
ENV CL_ALIAS ""
ENV CL_STOREPASS ""
ENV CL_KEYPASS ""

USER constellation

EXPOSE 9000
EXPOSE 9001
EXPOSE 9002
EXPOSE 9003

ENTRYPOINT java -Dlogback.configurationFile=/var/lib/constellation/logback.xml -Xmx$xmx -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -jar cl-node.jar --ip $ip --port 9000 -k $CL_KEYSTORE_NAME --alias $CL_ALIAS --whitelisting $CL_WHITELISTING_NAME
