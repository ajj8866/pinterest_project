from matplotlib.font_manager import ttfFontProperty
from pyrsistent import m
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
from pathlib import Path
import tempfile
import re
# from User_Emulation.user_posting_emulation import run_infinite_post_data_loop, AWSDBConnector
import os


class PySparkIntegrations:
    '''
    Class using pyspark for the purposes of reding data in from AWS s3 and Kafka
    '''
    def __init__(self) -> None:
        # Setting up schema for data to be read in from either s3 or Kafka 
        self.schema= StructType([StructField('category', StringType(), nullable=True), StructField('index', StringType(),nullable=True), StructField('unique_id', StringType(), nullable=False), 
            StructField('title', StringType(), nullable=True), StructField('description', StringType(), nullable=True), StructField('follower_count', StringType(), nullable=True), 
            StructField('tag_list', StringType(), nullable=True), StructField('is_image_or_video', StringType(), nullable=True), StructField('image_src', StringType(), nullable=True),StructField('downloaded', IntegerType()), StructField('save_location', StringType(), nullable=True)])

    def get_s3(self, hadoop_aws_version= '3.3.4',  s3_bucket='pinterest-data-bucket-0990123', region='us-east-1'):
        '''
        Method to read in data from AWS s3 

        Arguments
        ----------------------------------------
        hadoop_aws_version: Must be specified and passed in to the configi settings for pyspark given the versions for 
            hadoop-aws and hadooop-common must be identical
        s3_bucket: Bucket name from which to download csv files
        region: Region used in s3 bucket
        '''

        # Setting up connection to AWS s3 using boto3 cient enabling for low-level access to s3 resources
        self.bucket_name= s3_bucket
        self.aws_id= os.environ.get('AWS_ACCESS_ID')
        self.aws_secret= os.environ.get('AWS_SECRET')
        self.aws_client= boto3.client('s3', aws_access_key_id= self.aws_id, region_name=region,aws_secret_access_key=self.aws_secret)

        # Setting configuration parameters for spark session
        self.cfg= (
            pyspark.SparkConf().setMaster(f'local[{multiprocessing.cpu_count()}]')
            .setAppName('PinterestApp')
            .set('spark.jars.packages', f'org.apache.hadoop:hadoop-aws:{hadoop_aws_version}')
            .set('spark.jars.packages', f'org.apache.hadoop:hadoop-common:{hadoop_aws_version}')
            .set('spark.jars.packages', f'org.apache.hadoop:hadoop-client:{hadoop_aws_version}') # TBC
            .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            )

        self.spark_session= pyspark.sql.SparkSession.builder.config(conf=self.cfg).getOrCreate()

        # Accessing secret key and access id from S3 using context object
        sc= self.spark_session.sparkContext
        sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', self.aws_id) # 
        sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', self.aws_secret)
        # TBC
        sc._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')   
        sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        # sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')



        main_df= self.spark_session.createDataFrame(sc.parallelize([]), schema=self.schema)

        # Looping over obects in AWS s3 bucket to read in to Pyspark using temp module
        
        try:
            for obj in self.aws_client.list_objects_v2(Bucket=self.bucket_name)['Contents']:  
                # temp_df= self.spark_session.read.csv(f's3a:{self.bucket_name}/{csv_file}', header=True, inferSchema=True)
                temp_df= self.spark_session.read.format('csv').option('header', True).schema(self.schema).load(f's3a://{self.bucket_name}/{obj["Key"]}')
                main_df= main_df.union(temp_df)
                main_df.show()
                main_df.cache()
                time.sleep(2)
        except Exception as e:
            print(e)
            print('Trying to download on local directory and loading into spark dataframe')
            csv_holder= 'combined_csvs'
            counter= 1
            for obj in self.aws_client.list_objects_v2(Bucket=self.bucket_name)['Contents']:
                counter_z= str(counter).zfill(2)
                self.aws_client.download_file(self.bucket_name, obj['Key'], f'pinterest_{counter_z}.csv')
                temp_df= self.spark_session.read.csv(f'pinterest_{counter_z}.csv', schema= self.schema, header=True)
                main_df= main_df.union(temp_df)
                counter+=1
        
        main_df.show(20, truncate=True)
        # main_df.write.option('header', True).mode('overwrite').csv('combined_data')
        print('#'*20)
        print('Spark Dataframe Before Preprocessing')
        main_df.show(20, truncate=False)
        main_df.createOrReplaceTempView('pinterest_table')
        sql_query= self.spark_session.sql('SELECT DISTINCT SUBSTRING(follower_count, -1) FROM pinterest_table')
        sql_query.show()
        main_df= main_df.withColumn('follower_count', \
            when(main_df.follower_count.endswith('k'), regexp_replace(main_df.follower_count, 'k', '000')) \
            .when(main_df.follower_count.endswith('M'), regexp_replace(main_df.follower_count, 'M', '000000')) \
            .when(main_df.follower_count.endswith('r'), regexp_replace(main_df.follower_count, r'.*', '0')) \
            .when(main_df.follower_count.endswith('"'), regexp_replace(main_df.follower_count, r'.*', '0')).otherwise(main_df.follower_count))

        main_df= main_df.withColumn('follower_count', main_df.follower_count.cast('int'))
        print('#'*20)
        print('Spark Dataframe Before Preprocessing')
        main_df.show(20, truncate=False)
        main_df.write.options(header='True').mode('overwrite').csv('spark_df')
        for i in os.listdir():
            if re.findall(r'pinterest_\d{2}.csv', i)!=[]:
                os.unlink(i)
        return main_df
    
    def get_kafka(self, batch=False, topic_name='pinterest_topic'):
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0 pyspark-shell'
        self.cfg= (
            pyspark.SparkConf().setMaster(f'local[1]')
            .setAppName('Kafka_Pinterest')
        )
        self.spark_session= pyspark.sql.SparkSession.builder.config(conf=self.cfg).getOrCreate()
        if batch==False:
            kafka_df= self.spark_session.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subscribe', topic_name).option('kafka.group.id', 'stream_consumer').load()
        else:
            kafka_df= self.spark_session.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('startingOffsets', 'earliest').option('endingOffsets', 'latest').option('subscribe', topic_name).option('kafka.group.id', 'batch_consumer').load()

        kafka_df= kafka_df.selectExpr('CAST(value AS STRING)')
        kafka_df.writeStream.format('console').outputMode('append').start().awaitTermination()


if __name__=='__main__':
    spark_from_s3= PySparkIntegrations()
    spark_from_s3.get_s3()

