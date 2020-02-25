#!/bin/bash
set -e

# sbt assembly
docker run -ti --rm -v $(pwd):/app/clin-etl \
    -v ~/.m2:/root/.m2 \
    -v ~/.ivy2:/root/.ivy2 \
    -v ~/.sbt:/root/.sbt \
    -w /app/clin-etl hseeberger/scala-sbt:8u181_2.12.8_1.2.8 \
    sbt clean assembly

docker build -t clin-etl .
11.0.1_2.12.8_1.2.7