import imp
from click import option
from pip import main
import py
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
from threading import Event, Thread

from torch import StringType
from User_Emulation.user_posting_emulation import run_infinite_post_data_loop, AWSDBConnector
import os

class PySparkIntegrations:
    def __init__(self, s3_bucket='pinterest-data-bucket-0990123', region='us-east-1') -> None:
        self.bucket_name= s3_bucket
        self.aws_id= os.environ.get('AWS_ACCESS_ID')
        self.aws_secret= os.environ.get('AWS_SECRET')
        self.aws_client= boto3.client('s3', aws_access_key_id= self.aws_id, region_name=region,aws_secret_access_key=self.aws_secret)
        self.schema= StructType([StructField('category', StringType(), nullable=True), StructField('index', StringType(),nullable=True), StructField('unique_id', StringType(), nullable=False), 
            StructField('title', StringType(), nullable=True), StructField('description', StringType(), nullable=True), StructField('follower_count', StringType(), nullable=True), 
            StructField('tag_list', StringType(), nullable=True), StructField('is_image_or_video', StringType(), nullable=True), StructField('image_src', StringType(), nullable=True),StructField('downloaded', IntegerType()), StructField('save_location', StringType(), nullable=True)])

    def get_s3(self, hadoop_aws_version= '3.3.4'):
        # Setting configuration parameters for spark session
        self.cfg= (
            pyspark.SparkConf().setMaster(f'local[{multiprocessing.cpu_count()}]')
            .setAppName('PinterestApp')
            .set('spark.jars.packages', f'org.apache.hadoop:hadoop-aws:{hadoop_aws_version}')
            .set('spark.jars.packages', f'org.apache.hadoop:hadoop-common:{hadoop_aws_version}')
            )

        self.spark_session= pyspark.sql.SparkSession.builder.cofig(conf=self.cfg).getOrCreate()

        # Accessing secret key and access id from S3 using context object
        sc= self.spark_session.sparkContext
        sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', self.aws_id)
        sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', self.aws_secret)
        main_df= self.spark_session.createDataFrame(sc.parallelize([]), schema=self.schema)
        for csv_file in self.aws_client.list_objects_v2(Bucket=self.bucket_name)['Contents']['Key']:
            temp_df= self.spark_session.read.csv(f's3a:{self.bucket_name}/{csv_file}', header=True, inferSchema=True)
            main_df= main_df.union(temp_df)
        
        sql_view= main_df.createOrReplaceTempView('pinterest_table')
        sql_query= self.spark_session.sql('SELECT DISTINCT SUBSTRING(follower_count, -1) FROM pinterest_table')
        sql_query.show()
        main_df= main_df.withColumn('follower_count', \
            when(main_df.follower_count.endswith('k'), regexp_replace(main_df.follower_count, 'k', '000')) \
            .when(main_df.follower_count.endswith('M'), regexp_replace(main_df.follower_count, 'M', '000000')) \
            .when(main_df.follower_count.endswith('r'), regexp_replace(main_df.follower_count, r'.*', '0')) \
            .when(main_df.follower_count.endswith('"'), regexp_replace(main_df.follower_count, r'.*', '0')).otherwise(main_df.follower_count))

        main_df.show()

        main_df= main_df.withColumn('follower_count', main_df.follower_count.cast('int'))

    
    def get_kafka(self, topic_name='pinterest_topic'):
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 pyspark-shell'
        self.cfg= (
            pyspark.SparkConf().setMaster(f'local[1]')
            .setAppName('Kafka_Pinterest')
        )
        self.spark_session= pyspark.sql.SparkSession.builder.config(conf=self.cfg).getOrCreate()
        kafka_df= self.spark_session.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('startingOffsets', 'earliest').load()
        kafka_df= kafka_df.selectExpr('CAST(value AS STRING)')
        kafka_df.writeStream.format('console').outputMode('append').start().awaitTermination()



