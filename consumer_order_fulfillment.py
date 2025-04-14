# consumer_payment.py
import pika
import json
import random
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def fulfill_order(ch, method, properties, body):
    
    data = json.loads(body)
    order_id = data["order_id"]
    user_name = data["user_name"]
    # order fulfillment msg
    


    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # send to queue
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="order-fulfilled", body=json.dumps(data))
    
    print(f"[{user_name}] Fulfillment: Order {order_id} fulfilled. Events published.")
    connection.close()

def start_fulfill_consumer():
    #extended from consumer_payment: 
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="fulfillment_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="fulfillment_queue", routing_key="payment-applied")

    channel.basic_consume(queue="fulfillment_queue", on_message_callback=fulfill_order, auto_ack=True)

    print("Waiting for 'payment-applied' messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_fulfill_consumer()