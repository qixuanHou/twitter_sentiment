from storm import Spout, emit, log
from kafka.client import KafkaClient
from kafka import KafkaConsumer
from awscredentials import AWS_EC2_DNS
import json
import ast
consumer = KafkaConsumer("forestfire" ,bootstrap_servers=["ip:6667"])

def getData():
    data = consumer.next().value
    return data

class SensorSpout(Spout):
    def nextTuple(self):
        data = getData()
        log(data)
        emit([data])
        
SensorSpout().run()
