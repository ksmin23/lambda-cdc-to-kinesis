#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import os
import json

import dataset
import boto3

DRY_RUN = (os.getenv('DRY_RUN', 'false') == 'true')

AWS_REGION = os.getenv('REGION_NAME', 'us-east-1')
KINESIS_STREAM_NAME = os.getenv('KINESIS_STREAM_NAME', 'cdc-from-mysql')

DB_URL_FMT = 'mysql+pymysql://{db_user}:{db_password}@{db_host}/{database}'

QUERY_FMT = '''SELECT {columns}
FROM {database}.{table}
WHERE {where_clause}
'''

def write_records_to_kinesis(kinesis_client, kinesis_stream_name, records):
  import random
  random.seed(47)

  def gen_records():
    record_list = []
    for rec in records:
      payload = json.dumps(rec, ensure_ascii=False)
      partition_key = 'pk-{:05}'.format(random.randint(1, 1024))
      record_list.append({'Data': payload, 'PartitionKey': partition_key})
    return record_list

  MAX_RETRY_COUNT = 3

  record_list = gen_records()
  for _ in range(MAX_RETRY_COUNT):
    try:
      if DRY_RUN:
        print("[DEBUG] Kinesis PutRecords:\n", record_list, file=sys.stderr)
      else:
        response = kinesis_client.put_records(Records=record_list, StreamName=kinesis_stream_name)
        print("[DEBUG]", response, file=sys.stderr)
      break
    except Exception as ex:
      import time

      traceback.print_exc()
      time.sleep(2)
  else:
    raise RuntimeError('[ERROR] Failed to put_records into kinesis stream: {}'.format(kinesis_stream_name))


def lambda_handler(event, context):
    kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)

    for record in event['Records']:
      msg = json.loads(record['Sns']['Message'])
      db_url = DB_URL_FMT.format(**msg)
      db = dataset.connect(db_url)

      query = QUERY_FMT.format(**msg)
      print('[DEBUG] Query:\n{}'.format(query), file=sys.stderr)

      MAX_PUT_RECORDS = msg.get('kinesis_max_put_records', 100)

      record_list = []
      result_set = db.query(query)
      for row in result_set:
        #print('[DEBUG]', json.dumps(row, ensure_ascii=False))
        if len(record_list) == MAX_PUT_RECORDS:
          write_records_to_kinesis(kinesis_client, KINESIS_STREAM_NAME, record_list)
          record_list = [row]
        else:
          record_list.append(row)

      if record_list:
        #print('[DEBUG]', json.dumps(row, ensure_ascii=False))
        write_records_to_kinesis(kinesis_client, KINESIS_STREAM_NAME, record_list)


if __name__ == '__main__':
  import datetime

  test_sns_event = {
    "Records": [
      {
        "EventSource": "aws:sns",
        "EventVersion": "1.0",
        "EventSubscriptionArn": "arn:aws:sns:us-east-1:{{{accountId}}}:ExampleTopic",
        "Sns": {
          "Type": "Notification",
          "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
          "TopicArn": "arn:aws:sns:us-east-1:123456789012:ExampleTopic",
          "Subject": "example subject",
          "Message": "example message",
          "Timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
          "SignatureVersion": "1",
          "Signature": "EXAMPLE",
          "SigningCertUrl": "EXAMPLE",
          "UnsubscribeUrl": "EXAMPLE",
          "MessageAttributes": {
            "Test": {
              "Type": "String",
              "Value": "TestString"
            },
            "TestBinary": {
              "Type": "Binary",
              "Value": "TestBinary"
            }
          }
        }
      }
    ]
  }

  db_user = '<db_user>'
  db_password = '<db_password>'
  db_host = 'cdc-test.rds.amazonaws.com'
  my_database = 'test'
  my_table_name = 'pet'

  rds_cdc_query = {
    "db_host": db_host,
    "db_user": db_user,
    "db_password": db_password,
    "database": my_database,
    "table": my_table_name,
    "columns": "name, owner, species, sex, DATE_FORMAT(birth, '%Y-%m-%d') AS birth",
    "where_clause": "c_time >= '2020-06-06 13:23:00' AND c_time < '2020-06-06 13:24:00'",
    "primary_column": "pkid",
    "kinesis_max_put_records": 100
  }
  message = json.dumps(rds_cdc_query, ensure_ascii=False)

  test_sns_event['Records'][0]['Sns']['Subject'] = 'CDC from {database}.{table}'.format(database=my_database, table=my_table_name)
  test_sns_event['Records'][0]['Sns']['Message'] = message

  lambda_handler(test_sns_event, {})

