# consumer_notification.py
import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def send_notification(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]
    
    if method.routing_key == "order-created":
        message = f"Order {order_id} has been placed successfully. Notification sent to user."
    elif method.routing_key == "payment-success":
        message = f"Payment for Order {order_id} was successful. Notification sent to user."
    elif method.routing_key == "payment-denied":
        message = f"Payment for Order {order_id} was denied. Notification sent to user."
    else:
        message = "Unknown notification event received."

    print(message)

def start_notification_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="notification_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key="order-created")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key="payment-success")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key="payment-denied")

    channel.basic_consume(queue="notification_queue", on_message_callback=send_notification, auto_ack=True)

    print("Waiting for notification messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_notification_consumer()