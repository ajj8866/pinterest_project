from re import U
from fastapi import FastAPI
from pydantic import BaseModel
import numpy as  np
import uvicorn
from json import dumps
from typing import Optional
import uuid
from kafka import KafkaProducer

app = FastAPI()

# def producer_post(key=None, value=None, topic_name= 'MyFirstKafkaTopic'):
#     producer= KafkaProducer(bootstrap_servers='localhost:9092')
#     for enum, msg in enumerate(range(20)):
#         producer.send(topic=topic_name, key=str('message number', enum).encode(), value=str(np.random.random()).encode())
#         # producer.send(topic=topic_name, key=str(key).encode(), value=str(value).encode())

producer= KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda i: dumps(i).encode('ascii'))

class Data(BaseModel):
    category: str
    index_1: int
    unique_id: Optional[str]= uuid.uuid4()
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

info_dict= {}

@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer.send(topic='pinterest_topic', value=data)
    # producer.flush()
    print(producer.metrics)
    return data


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
