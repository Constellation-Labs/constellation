#!/usr/bin/env bash

JAR_TAG=${1:-dev}

sbt assembly && \
gsutil cp target/scala-2.12/constellation-assembly-1.0.1.jar gs://constellation-dag/release/dag-$JAR_TAG.jar && \
gsutil acl ch -u AllUsers:R gs://constellation-dag/release/dag-$JAR_TAG.jar