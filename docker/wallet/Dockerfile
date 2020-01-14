FROM openjdk:8-jdk-alpine

RUN adduser -S -h /var/lib/constellation constellation

COPY constellation-node.jar /var/lib/constellation/constellation-node.jar
COPY logback.xml /var/lib/constellation/logback.xml

WORKDIR /var/lib/constellation/shared

USER constellation

ENTRYPOINT ["java", "-Dlogback.configurationFile=/var/lib/constellation/logback.xml", "-cp", "../constellation-node.jar"]
CMD ["org.constellation.util.wallet.GenerateAddress"]
