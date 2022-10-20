from pyspark.sql.types import StructField, StringType, IntegerType, StructType
import multiprocessing
import pyspark
from pyspark.sql.functions import col, when, regexp_replace
import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
import time
from pyspark_s3 import PySparkIntegrations

class SparkCassandra(PySparkIntegrations):
    def __init__(self) -> None:
        super().__init__()
    

    def cassandra_connect(self):
        # self.cfg= pyspark.SparkConf().setMaster('local[1]'). \
        #     set("com.datastax.spark:spark-cassandra-connector_2.12:3.2.0")
        self.cfg= (pyspark.SparkConf().setMaster('local[1]').setAppName('Cassan').set('spark.cassandra.connection.host', 'localhost').set('spark.cassandra.connection.port', '9042'))
        self.spark_session= pyspark.sql.builder.conf(conf= self.cfg).getOrCreate()
