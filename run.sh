#!/usr/bin/env bash
docker run -ti --network container:es_elasticsearch.lh3acahhjkznoe7072mh825ds.usc45xu2bdtpz1nw65kg9bslx -v $(pwd)/ndjson:/ndjson -e es.nodes=elasticsearch -e es.port=9200 clin-etl