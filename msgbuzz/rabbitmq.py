import json
import signal
import sys

import pika
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from msgbuzz import MessageBus, ConsumerConfirm, Message


class RabbitMqMessageBus(MessageBus):

    def __init__(self, host='localhost'):
        self._subscribers = {}
        self._conn_params = pika.ConnectionParameters(host=host)
        self._conn = pika.BlockingConnection(self._conn_params)

    def publish(self, topic_name, body, headers=None):
        channel = self._conn.channel()
        message = Message(headers, body)
        msg_json = json.dumps(message.__dict__)
        channel.basic_publish(exchange=topic_name, routing_key='', body=msg_json,
                              properties=BasicProperties(content_type="application/json"))

    def on(self, topic_name, client_group, callback):
        self._subscribers[topic_name] = (client_group, callback)

    def start_consuming(self):
        signal.signal(signal.SIGINT, self._signal_handler)

        for topic_name, (client_group, callback) in self._subscribers.items():
            conn_params = pika.ConnectionParameters('localhost')
            RabbitMqConsumer.consume(conn_params, topic_name, client_group, callback)

    def _signal_handler(self, sig, frame):
        print('You pressed Ctrl+C!')
        # TODO gracefully shutdown consumer here
        sys.exit(0)


class RabbitMqConsumerConfirm(ConsumerConfirm):

    def __init__(self, channel: Channel, delivery: Basic.Deliver):
        self._channel = channel
        self._delivery = delivery

    def ack(self):
        self._channel.basic_ack(self._delivery.delivery_tag)

    def nack(self):
        self._channel.basic_nack(self._delivery.delivery_tag)


class RabbitMqConsumer:

    @staticmethod
    def consume(conn_params, topic_name, client_group, callback):
        # create new conn
        # rabbitmq best practice 1 process 1 conn, 1 thread 1 channel
        conn = pika.BlockingConnection(conn_params)

        # create channel
        channel = conn.channel()

        # create exchange for pub/sub
        channel.exchange_declare(exchange=topic_name, exchange_type='fanout')

        # create dedicated queue for receiving message (create subscriber)
        queue_name = f'{topic_name}.{client_group}'
        channel.queue_declare(queue=queue_name, arguments={"x-dead-letter-exchange": topic_name})

        # bind created queue with pub/sub exchange
        channel.queue_bind(exchange=topic_name, queue=queue_name)

        # set consume callback
        channel.basic_consume(queue=queue_name, on_message_callback=RabbitMqConsumer._callback_wrapper(callback),
                              auto_ack=False)

        # start consuming (blocking)
        print("Waiting incoming message. To exit press Ctrl+C")
        channel.start_consuming()

    @staticmethod
    def _callback_wrapper(callback):
        def fn(ch, method, properties, body):
            msg_dict = json.loads(body)
            msg = Message(msg_dict.get("headers"), msg_dict.get("body"))
            if type(msg.headers) == dict:
                msg.headers["x-rabbit"] = properties.headers
            callback(RabbitMqConsumerConfirm(ch, method), msg)

        return fn
