# import kafka
from time import time
from kafka import KafkaConsumer
from kafka import KafkaProducer
from project_pin_API import producer, Data
import json
import boto3
import uuid
import signal
from queue import Queue, Empty
import threading
import time
import logging
import tempfile
import csv
# consumer_stream= KafkaConsumer('pinterest_topic', group_id='pinterest_streamer_group', bootstrap_servers='localhost:9092', value_deserializer=lambda i: json.loads(i.decode('ascii')), 
# enable_auto_commit=True, auto_offset_reset= 'latest')
logging.basicConfig()

with open('json_conf.json', 'r') as j:
    conf_file_s3= json.load(j)
    key_id = conf_file_s3['s3']["access_key_id"]
    secret_key = conf_file_s3['s3']["secret_access_key"]


msges= Queue()

class BatchConsumer(threading.Thread):
    def __init__(self) -> None:
        threading.Thread.__init__(self)
    
    def run(self):
        consumer_batch= KafkaConsumer('pinterest_topic', auto_offset_reset='earliest',
         enable_auto_commit= True, group_id='batch_consumer', 
         value_deserializer=lambda i: json.loads(i.decode('ascii')), 
         bootstrap_servers='localhost:9092')

        for msg in consumer_batch:
            msges.put(msg.value)
            print(f'''
            1) Topic: {msg.topic}
            2) Partition: {msg.partition}
            3) Offset: {msg.offset}
            4) Key: {msg.key}
            5) Value: {msg.value}
            ''')
        
        consumer_batch.close()
    

def process_msg_q(region='us-east-1', json=False):
    temp_ls= []
    try: 
        while True:
            temp_ls.append(msges.get_nowait())
    
    except Empty:
        pass
    aws_client= boto3.client('s3', aws_access_key_id= key_id,region_name= region, aws_secret_access_key=secret_key)
    new_uuid= uuid.uuid4()
    bucket_name= f'pinterest-data-bucket-0990123'
    # location= {'LocationConstraint': region}
    aws_client.create_bucket(Bucket=bucket_name) #, CreateBucketConfiguration=location)
    print('#'*20)
    print(len(temp_ls))
    print('#'*20)
    with tempfile.TemporaryDirectory(dir='.') as temp_dir:
        if json==False:
        # print('Temporaray Directory Name: ', temp_dir.name)
            with open(f'{temp_dir}/pinterest_{new_uuid}.csv', mode='w') as csv_file:
                field_names= ['category', 'index', 'unique_id', 'title', 'description', 'follower_count', 'tag_list', 'is_image_or_video', 'image_src', 'downloaded','save_location']
                csv_writer= csv.DictWriter(csv_file, fieldnames=field_names)
                csv_writer.writeheader()
                for row in temp_ls:
                    csv_writer.writerow({'category': row['category'], 'unique_id': row['unique_id'], 'title': row['title'], 'description': row['description'], 'follower_count': row['follower_count'], 'tag_list': row['tag_list'], 'is_image_or_video': row['is_image_or_video'], 'downloaded': row['downloaded'], 'save_location': row['save_location']})
            aws_client.upload_file(f'{temp_dir}/pinterest_{new_uuid}.csv', 'pinterest-data-bucket-0990123', f'pinterest_{new_uuid}.csv')
        else:
            with open(f'{temp_dir}/pinterest-json-{new_uuid}.json', mode='w') as json_file:
                json.dump(temp_ls, json_file)
        # temp_dir.cleanup()
            
            
# AWS_REGION= 'us-east-1'
# aws_client= boto3.client('s3', region_name=AWS_REGION)

if __name__=='__main__':
    print('Starting batch consumer')
    consumer= BatchConsumer()
    consumer.daemon= True
    consumer.start()
    
    while True:
        process_msg_q(region='us-east-1')
        time.sleep(15)


