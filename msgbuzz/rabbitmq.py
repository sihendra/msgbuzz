import json
import logging
import multiprocessing
import signal

import pika
from pika.channel import Channel
from pika.spec import Basic, BasicProperties

from msgbuzz import MessageBus, ConsumerConfirm, Message

_logger = logging.getLogger(__name__)


class RabbitMqMessageBus(MessageBus):

    def __init__(self, host='localhost', port=5672):
        self._subscribers = {}
        self._conn_params = pika.ConnectionParameters(host=host, port=port)

        self._conn = pika.BlockingConnection(self._conn_params)
        self._consumers = []

    def publish(self, topic_name, message: Message):
        channel = self._conn.channel()
        msg_json = json.dumps(message.__dict__)
        channel.basic_publish(exchange=topic_name, routing_key='', body=msg_json,
                              properties=BasicProperties(content_type="application/json"))

    def on(self, topic_name, client_group, callback):
        self._subscribers[topic_name] = (client_group, callback)

    def start_consuming(self):
        consumer_count = len(self._subscribers.items())
        if consumer_count == 0:
            return

        signal.signal(signal.SIGINT, self._signal_handler)

        # one consumer just use current process
        if consumer_count == 1:
            topic_name = next(iter(self._subscribers))
            client_group, callback = self._subscribers[topic_name]
            consumer = RabbitMqConsumer(self._conn_params, topic_name, client_group, callback)
            self._consumers.append(consumer)
            consumer.run()
            return

        # multiple consumers use child process
        for topic_name, (client_group, callback) in self._subscribers.items():
            consumer = RabbitMqConsumer(self._conn_params, topic_name, client_group, callback)
            self._consumers.append(consumer)
            consumer.start()

        for consumer in self._consumers:
            consumer.join()

    def _signal_handler(self, sig, frame):
        _logger.info(f"You pressed Ctrl+C!")
        for consumer in self._consumers:
            consumer.stop()

        _logger.info("Stopping consumers")


class RabbitMqConsumer(multiprocessing.Process):

    def __init__(self, conn_params, topic_name, client_group, callback):
        super().__init__()
        self._conn_params = conn_params
        self._topic_name = topic_name
        self._client_group = client_group
        self._name_generator = RabbitMqQueueNameGenerator(topic_name, client_group)
        self._is_interrupted = False
        self._callback = callback

    def stop(self):
        self._is_interrupted = True

    def run(self):
        # create new conn
        # rabbitmq best practice 1 process 1 conn, 1 thread 1 channel
        conn = pika.BlockingConnection(self._conn_params)

        # create channel
        channel = conn.channel()

        q_names = self._name_generator
        # create dlx exchange and queue
        channel.exchange_declare(exchange=q_names.dlx_exchange(), durable=True)
        channel.queue_declare(queue=q_names.dlx_queue_name(), durable=True)
        channel.queue_bind(exchange=q_names.dlx_exchange(), queue=q_names.dlx_queue_name())

        # create exchange for pub/sub
        channel.exchange_declare(exchange=q_names.exchange_name(), exchange_type='fanout', durable=True)
        # create dedicated queue for receiving message (create subscriber)
        channel.queue_declare(queue=q_names.queue_name(),
                              durable=True,
                              arguments={'x-dead-letter-exchange': q_names.dlx_exchange(),
                                         'x-dead-letter-routing-key': q_names.dlx_queue_name()})
        # bind created queue with pub/sub exchange
        channel.queue_bind(exchange=q_names.exchange_name(), queue=q_names.queue_name())

        # setup retry requeue exchange and binding
        channel.exchange_declare(exchange=q_names.retry_exchange(), durable=True)
        channel.queue_bind(exchange=q_names.retry_exchange(), queue=q_names.queue_name())
        # create retry queue
        channel.queue_declare(queue=q_names.retry_queue_name(),
                              durable=True,
                              arguments={
                                  "x-dead-letter-exchange": q_names.retry_exchange(),
                                  "x-dead-letter-routing-key": q_names.queue_name()})

        # start consuming
        _logger.info(f"Waiting incoming message for topic: {q_names.exchange_name()}. To exit press Ctrl+C")

        wrapped_callback = _callback_wrapper(self._name_generator, self._callback)
        for message in channel.consume(queue=q_names.queue_name(), auto_ack=False, inactivity_timeout=1):
            if self._is_interrupted:
                break

            if not message:
                continue

            method, properties, body = message

            if method is None:
                continue

            if self._message_expired(properties):
                _logger.debug(f"Max retry reached dropping msg {properties}")
                channel.basic_nack(method.delivery_tag, requeue=False)
                continue

            try:
                wrapped_callback(channel, method, properties, body)
            except Exception:
                _logger.exception("Exception when calling callback")

        _logger.info(f"Consumer stopped")

    @staticmethod
    def _message_expired(properties):
        return properties.headers \
               and properties.headers.get("x-death") \
               and properties.headers.get("x-max-retries") \
               and properties.headers.get("x-death")[0]["count"] > properties.headers.get("x-max-retries")


class RabbitMqQueueNameGenerator:

    def __init__(self, topic_name, client_group):
        self._client_group = client_group
        self._topic_name = topic_name

    def exchange_name(self):
        return self._topic_name

    def queue_name(self):
        return f'{self._topic_name}.{self._client_group}'

    def retry_exchange(self):
        return self.retry_queue_name()

    def retry_queue_name(self):
        return f"{self.queue_name()}__retry"

    def dlx_exchange(self):
        return self.dlx_queue_name()

    def dlx_queue_name(self):
        return f"{self.queue_name()}__failed"


class RabbitMqConsumerConfirm(ConsumerConfirm):

    def __init__(self, names_gen: RabbitMqQueueNameGenerator, channel: Channel, delivery: Basic.Deliver,
                 properties: BasicProperties, body):
        """
        Create instance of RabbitMqConsumerConfirm

        :param names_gen: QueueNameGenerator
        :param channel:
        :param delivery:
        :param properties:
        :param body:
        """
        self.names_gen = names_gen

        self._channel = channel
        self._delivery = delivery
        self._properties = properties
        self._body = body

    def ack(self):
        self._channel.basic_ack(delivery_tag=self._delivery.delivery_tag)

    def nack(self):
        self._channel.basic_nack(delivery_tag=self._delivery.delivery_tag, requeue=False)

    def retry(self, delay=60000, max_retries=3):
        # RabbitMq expiration should be str
        self._properties.expiration = str(delay)

        if self._properties.headers is None:
            self._properties.headers = {}
        self._properties.headers['x-max-retries'] = max_retries

        q_names = self.names_gen
        _logger.info(f"About to retry body:{self._body} props: {self._properties}")
        self._channel.basic_publish("", q_names.retry_queue_name(), self._body, properties=self._properties)

        self.ack()


def _callback_wrapper(names_gen: RabbitMqQueueNameGenerator, callback):
    """
    Wrapper for callback. since nested function cannot be pickled, we need some top level function to wrap it

    :param callback:
    :return: function
    """

    def fn(ch, method, properties, body):
        msg_dict = json.loads(body)
        msg = Message(msg_dict.get("headers"), msg_dict.get("body"))
        callback(RabbitMqConsumerConfirm(names_gen, ch, method, properties, body), msg)

    return fn
