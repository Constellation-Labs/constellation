FROM openjdk:8-jdk-alpine

RUN adduser -S -h /var/lib/constellation constellation

COPY constellation-node.jar /var/lib/constellation/constellation-node.jar
COPY logback.xml /var/lib/constellation/logback.xml

WORKDIR /var/lib/constellation/

ENV ip "127.0.0.1"
ENV xmx 5G

USER constellation

EXPOSE 9000
EXPOSE 9001

ENTRYPOINT java -Dlogback.configurationFile=/var/lib/constellation/logback.xml -Xmx$xmx -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -jar constellation-node.jar --ip $ip --port 9000