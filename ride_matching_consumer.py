# Code for Ride Matching Consumer
import json
import os
import pika
import requests
import time

# getting the consumer id defined in the dockerfile as well as the producer address
CONSUMER_ID = os.environ['CONSUMER_ID']
PRODUCER_ADDRESS = os.environ['PRODUCER_ADDRESS']

# time taken to serve each consumer 
time.sleep(10)

response = requests.post(
    f'http://{PRODUCER_ADDRESS}/new_ride_matching_consumer',
    data=f'consumer_id={CONSUMER_ID}')

assert response.status_code == 200

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

# creating the ride match queue 
# output example :
# Consumer C2 started...
channel.queue_declare(queue='ride_match', durable=True)
print(f'Consumer {CONSUMER_ID} started...', flush=True)


def callback(ch, method, properties, body):
    body = json.loads(body.decode())
    body['_id'] = properties.message_id
    print(f'Consumed {body} from ride_match queue', flush=True)
    # Consumed {'location': 'vegacity', 'destination': 'xyz', 'time': '7', 'seats': '2', 'cost': '150', '_id': 'task_61e696fea3134a76939d97033f76335a'} from ride_match queue
    time.sleep(int(body['time']))
    print(f'Consumer ID: {CONSUMER_ID}, Task ID: {properties.message_id}', flush=True)
    # Consumer ID: C2, Task ID: task_61e696fea3134a76939d97033f76335a
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='ride_match', on_message_callback=callback)
channel.start_consuming()