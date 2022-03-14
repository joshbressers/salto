#!/usr/bin/env python

import os
import sys
import json
import socket
import glob
import boto3
import time
from s3logparse import s3logparse
import elasticsearch
import elasticsearch.helpers
from elasticsearch import Elasticsearch
import threading, queue

s3_q = queue.Queue(maxsize=10000)
es_q = queue.Queue(maxsize=3000)

def s3_worker():

    session = boto3.session.Session()
    s3 = session.resource('s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        aws_session_token=os.environ['AWS_SESSION_TOKEN'],
    )

    bucket = 'toolbox-data.anchore.io-logs'

    while True:
        key = s3_q.get()

        obj = s3.Object(bucket, key)

        body = obj.get()['Body'].read()

        line = body.decode('utf-8')
        lines = line.split("\n")

        if lines[-1] == '':
            lines = lines[0:-1]

        print(key)

        count = 0
        for line in s3logparse.parse_log_lines(lines):
            count = count + 1
            data = {}

            data['id'] = obj.key + "-" + str(count)
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
                if len(uri) > 2:
                    data['request_verb'] = uri[0]
                    data['request_uri'] = uri[1]
                    data['request_ver'] = uri[2]

            data['status_code'] = line.status_code
            data['error_code'] = line.error_code
            data['bytes_sent'] = line.bytes_sent
            data['user_agent'] = line.user_agent
            data['total_time'] = line.total_time
            data['turn_around_time'] = line.turn_around_time
            data['referrer'] = line.referrer
            data['version_id'] = line.version_id

            aws_bulk = {
                "_op_type": "update",
                "_index":   "aws",
                "_id":      data['id'],
                "doc_as_upsert": True,
                "pipeline": "aws",
                "doc":  data
            }

            es_q.put(aws_bulk)
        s3_q.task_done()

def es_worker():
    if 'ESURL' not in os.environ:
        es_url = "http://localhost:9200"
    else:
        es_url = os.environ['ESURL']

    es = Elasticsearch([es_url])

    # First let's see if the index exists
    if es.indices.exists(index='aws') is False:
        # We have to create it and add a mapping
        fh = open('mapping.json')
        mapping = json.load(fh)
        es.indices.create(index='aws', body=mapping)

    bulk_data = []
    last_run = False
    while True:
        obj = es_q.get()
        if obj == "Done":
            last_run = True
        else:
            bulk_data.append(obj)

        if last_run or len(bulk_data) >= 2000:
            for ok, item in elasticsearch.helpers.streaming_bulk(es, bulk_data, max_retries=2):
                if not ok:
                    print("ERROR:")
                    print(item)
            bulk_data = []
        es_q.task_done()

#path = sys.argv[1]


#for file in glob.glob(os.path.join(path, "*")):
#    with open(file) as fh:

threading.Thread(target=es_worker, daemon=True).start()

for i in range(0, 20):
    threading.Thread(target=s3_worker, daemon=True).start()

session = boto3.Session(
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    aws_session_token=os.environ['AWS_SESSION_TOKEN']
)

s3 = session.resource('s3')
bucket = s3.Bucket('toolbox-data.anchore.io-logs')

skip = 'access_logs/%s' % sys.argv[1]

#for obj in bucket.objects.all():
for obj in bucket.objects.filter(Prefix=skip):
    s3_q.put(obj.key)

while not s3_q.empty():
    time.sleep(1)

es_q.put("Done")
while not es_q.empty():
    time.sleep(1)

