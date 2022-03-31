# Use this file to setup the database consumer that stores the ride information in the database

import json
import pika # used to create message queues 
import pymongo
import time # this is to make the process sleep for each request 

time.sleep(10)

client = pymongo.MongoClient("mongodb://mongodb:27017") # connecting to the database 
db = client['ride_matching'] # name of schema 
collection = db['ride_details'] # name of database 


# establishes connection with the rabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# here we declare the name of the queue called database 

channel.queue_declare(queue='database', durable=True)

# once the queue is ready we print this message
print('Database consumer started...', flush=True)


# consumed {'location': 'vegacity', 'destination': 'stories', 'time': '7', 'seats': '2', 'cost': '150', '_id': 'task_3d0b29a347cc4729aba3a2949cf1ea9d'} from database queue
def callback(ch, method, properties, body):
    body = json.loads(body) # getting load from the curl query 
    body['_id'] = properties.message_id # to get the id we get it as the messahe id 
    print(f'Consumed {body} from database queue', flush=True)

    collection.insert_one(body)

    ch.basic_ack(delivery_tag=method.delivery_tag) 


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='database', on_message_callback=callback)

channel.start_consuming()