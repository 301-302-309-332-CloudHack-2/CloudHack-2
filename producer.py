# Code for Producer
from flask import Flask, request
import json
import pika # used for making queues
from uuid import uuid4

consumers = [] # list of consumers who'll go to the queue

app = Flask(__name__)   # flask constructor 

@app.route('/new_ride', methods=['POST']) # 
def new_ride():
    body = json.dumps(request.form)
    task_id = 'task_' + uuid4().hex # to create a unique hex id 

    # Connect to rabbitmq over tcp
    # guest connection 
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    
    # Create channel to publish to ride_match queue
    channel = connection.channel()
    channel.queue_declare(queue='ride_match', durable=True)
    # publishes a message to the ride match queue 
    channel.basic_publish(
        exchange='',
        routing_key='ride_match',
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
            message_id=task_id
        ))
    
    # Received task {"location": "vegacity", "destination": "stories", "time": "7", "seats": "2", "cost": "150"} via POST, published to queue
    print(f'Received task {body} via POST, published to queue', flush=True)

    # Create new channel to publish to database queue
    channel = connection.channel()
    # publishing request to the database queue 
    channel.queue_declare(queue='database', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='database',
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE,
            message_id=task_id
        ))

    connection.close()

    return ''


@app.route('/new_ride_matching_consumer', methods=['POST'])
def new_ride_matching_consumer():
    consumers.append({**request.form, 'ip_address': request.remote_addr})
    
    print(f'List of consumers: {consumers}', flush=True)

    return ''