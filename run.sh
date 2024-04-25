#!/bin/bash

export ESURL='https://elastic:zenk12@elasticsearch8:9200'
export ESCERT="/home/bress/es_ca.crt"

#YESTERDAY=`date --date=yesterday +"%Y-%m-%d"`
YESTERDAY=`date +"%Y-%m-%d"`

source /home/bress/src/salte/.venv/bin/activate

/home/bress/src/salte/parse-log-rollup.py $YESTERDAY
