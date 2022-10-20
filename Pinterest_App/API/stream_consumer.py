# import kafka
from kafka import KafkaConsumer
from kafka import KafkaProducer
from project_pin_API import producer, Data
import json
# from pyspark_s3 import PySparkIntegrations


class StreamConsumer:
    def __init__(self) -> None:
        
        self.consumer= KafkaConsumer('pinterest_topic', group_id='stream_consumer', bootstrap_servers='localhost:9092', value_deserializer=lambda i: json.loads(i.decode('ascii')), 
        enable_auto_commit=True, auto_offset_reset='latest')

if __name__=='__main__':
    pass
    # for msg in consumer:
    #     print(f'''
    #     1) Topic: {msg.topic}
    #     2) Partition: {msg.partition}
    #     3) Offset: {msg.offset}
    #     4) Key: {msg.key}
    #     5) Value: {msg.value}
    #     ''')


