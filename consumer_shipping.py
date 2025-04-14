# consumer_shipping.py
import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME


def shipping_order(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]
    user_name = data["user_name"]


    # order-shipped event
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="order-shipped", body=json.dumps(data))
    
    print(f"[{user_name}] Shipping: Order {order_id} shipped. Events published.")
    connection.close()

def start_shipping_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="shipping_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="shipping_queue", routing_key="order-fulfilled")

    channel.basic_consume(queue="shipping_queue", on_message_callback=shipping_order, auto_ack=True)

    print("Waiting for 'order-fulfilled' messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_shipping_consumer()