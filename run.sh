#!/usr/bin/env bash
docker run -ti --network container:es_elasticsearch.lh3acahhjkznoe7072mh825ds.qsgwb2j7cbl6nw8ru4zpbsrjn -v $(pwd)/ndjson:/ndjson -e es.nodes=elasticsearch -e es.port=9200 clin-etl
