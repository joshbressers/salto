#!/usr/bin/env python

import os
import sys
import json
import socket
import glob
import boto3
import time
from s3logparse import s3logparse
import threading, queue
import datetime
from esbulkstream import Documents


lock = threading.Lock()
syft_data = {}
grype_data = {}

s3_q = queue.Queue(maxsize=10000)

def parse_one_line(obj, line, count):
    data = {}

    #data['timestamp'] = line.timestamp.isoformat()

    ts = line.timestamp
    the_time = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, 0, 0)

    #data['operation'] = line.operation
    data['request_uri'] = line.request_uri

    if line.request_uri is not None:
        uri = line.request_uri.split(' ')
        if len(uri) > 2:
            #data['request_verb'] = uri[0]
            data['request_uri'] = uri[1]
            #data['request_ver'] = uri[2]

    if "syft" in data["request_uri"]:
        if the_time in syft_data:
            lock.acquire()
            syft_data[the_time] = syft_data[the_time] + 1
            lock.release()
        else:
            lock.acquire()
            syft_data[the_time] = 1
            lock.release()
    elif "grype" in data["request_uri"]:
        if the_time in grype_data:
            lock.acquire()
            grype_data[the_time] = grype_data[the_time] + 1
            lock.release()
        else:
            lock.acquire()
            grype_data[the_time] = 1
            lock.release()

def s3_worker():

    session = boto3.session.Session()
    s3 = session.resource('s3')

    bucket = 'toolbox-data.anchore.io-logs'

    while True:
        key = s3_q.get()

        try:
            obj = s3.Object(bucket, key)

            body = obj.get()['Body'].read()

            line = body.decode('utf-8')
            lines = line.split("\n")

            if lines[-1] == '':
                lines = lines[0:-1]

            q_size = s3_q.qsize()
            print(f"{key} {q_size}")

            count = 0
            for line in s3logparse.parse_log_lines(lines):
                count = count + 1

                parse_one_line(obj, line, count)

                #es_q.put(aws_bulk)
        except:
            # Sometimes this fails, just ignore it
            pass
        s3_q.task_done()


for i in range(0, 30):
    threading.Thread(target=s3_worker, daemon=True).start()

session = boto3.Session()
s3 = session.resource('s3')
bucket = s3.Bucket('toolbox-data.anchore.io-logs')

skip = 'access_logs/%s' % sys.argv[1]

#for obj in bucket.objects.all():
for obj in bucket.objects.filter(Prefix=skip):
    s3_q.put(obj.key)

while not s3_q.empty():
    time.sleep(1)


es = Documents('aws-rollup', mapping='', delete=False)

for i in syft_data:
    entry = { "timestamp": i, "hits": syft_data[i], "app": "syft" }
    doc_id = f"syft-{i}"
    error = es.add(entry, doc_id)

for i in grype_data:
    entry = { "timestamp": i, "hits": grype_data[i], "app": "grype" }
    doc_id = f"grype-{i}"
    error = es.add(entry, doc_id)

es.done()
