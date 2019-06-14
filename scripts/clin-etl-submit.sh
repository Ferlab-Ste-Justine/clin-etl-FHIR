#!/bin/bash

./spark/bin/spark-submit --master local[*] --class org.crsj.clin.etl.Main --conf "es.nodes=${ES_NODES}" --conf "es.port=${ES_PORT}" clin-etl.jar

