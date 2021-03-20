ARG BASE_IMAGE=amazoncorretto:8-alpine
FROM $BASE_IMAGE

RUN adduser -S -h /var/lib/constellation constellation

COPY cl-wallet.jar /var/lib/constellation/cl-wallet.jar
COPY entrypoint.sh /var/lib/constellation/entrypoint.sh

WORKDIR /var/lib/constellation/shared

ENV CL_KEYSTORE_NAME ""
ENV CL_ALIAS ""
ENV CL_STOREPASS ""
ENV CL_KEYPASS ""

USER constellation

ENTRYPOINT ["sh", "../entrypoint.sh"]
