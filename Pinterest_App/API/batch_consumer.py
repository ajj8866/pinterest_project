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
# consumer_stream= KafkaConsumer('pinterest_topic', group_id='pinterest_streamer_group', bootstrap_servers='localhost:9092', value_deserializer=lambda i: json.loads(i.decode('ascii')), 
# enable_auto_commit=True, auto_offset_reset= 'latest')


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
        
        consumer_batch.close()
    
    

def process_msg_q(region='us-east-1'):
    temp_ls= []
    try: 
        while True:
            temp_ls.append(msges.get_nowait())
    
    except Empty:
        pass
    aws_client= boto3.client('s3', region_name= region)
    bucket_name= f'pinterest_data_{uuid.uuid4()}'
    location= {'LocationConstraint': region}
    aws_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)


# AWS_REGION= 'us-east-1'
# aws_client= boto3.client('s3', region_name=AWS_REGION)

if __name__=='__main__':
    print('Starting batch consumer')
    consumer= BatchConsumer()
    consumer.daemon= True
    consumer.start()
    
    while True:
        process_msg_q(region='us-east-1')
        for msg in consumer:
            print(f'''
            1) Topic: {msg.topic}
            2) Partition: {msg.partition}
            3) Offset: {msg.offset}
            4) Key: {msg.key}
            5) Value: {msg.value}
            ''')
        time.sleep(10)


