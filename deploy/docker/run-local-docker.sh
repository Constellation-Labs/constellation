#!/usr/bin/env bash
docker run -it --rm --name constellation -p 9000:9000 -p 6006:6006 -p 9010:9010 -p 16180:16180/udp constellationlabs/constellation:latest
