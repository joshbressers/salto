#!/usr/bin/env python

import os
import sys
import json
import socket
import glob
from s3logparse import s3logparse
import elasticsearch
import elasticsearch.helpers
from elasticsearch import Elasticsearch

path = sys.argv[1]

if 'ESURL' not in os.environ:
    es_url = "http://localhost:9200"
else:
    es_url = os.environ['ESURL']

es = Elasticsearch([es_url])

bulk_data = []

# First let's see if the index exists
if es.indices.exists(index='aws') is False:
    # We have to create it and add a mapping
    fh = open('mapping.json')
    mapping = json.load(fh)
    es.indices.create(index='aws', body=mapping)

for file in glob.glob(os.path.join(path, "*")):
    with open(file) as fh:

        for line in s3logparse.parse_log_lines(fh.readlines()):
            data = {}

            data['id'] = line.request_id
            data['bucket'] = line.bucket
            data['timestamp'] = line.timestamp.isoformat()
            data['remote_ip'] = line.remote_ip
            #try:
            #    data['dns'] = socket.gethostbyaddr(line.remote_ip)[0]
            #except:
            #    pass
            data['operation'] = line.operation
            data['s3_key'] = line.s3_key
            data['request_uri'] = line.request_uri

            if line.request_uri is not None:
                uri = line.request_uri.split(' ')
                if len(uri) > 3:
                    data['request_verb'] = uri[0]
                    data['request_uri'] = uri[1]
                    data['request_ver'] = uri[2]

            data['bytes_sent'] = line.bytes_sent
            data['user_agent'] = line.user_agent

            aws_bulk = {
                "_op_type": "update",
                "_index":   "aws",
                "_id":      data['id'],
                "doc_as_upsert": True,
                "doc":  data
            }

            bulk_data.append(aws_bulk)

            if len(bulk_data) > 1000:
                for ok, item in elasticsearch.helpers.streaming_bulk(es, bulk_data, max_retries=2):
                    if not ok:
                        print("ERROR:")
                        print(item)
                bulk_data = []

            #es.index(id=data['id'], index="aws", document=data, pipeline="geoip")
