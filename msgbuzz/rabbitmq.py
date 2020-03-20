import logging
import multiprocessing
import threading
import time

import pika
from pika.channel import Channel
from pika.exceptions import ConnectionClosed, StreamLostError
from pika.spec import Basic, BasicProperties

from msgbuzz import MessageBus, ConsumerConfirm

_logger = logging.getLogger(__name__)


class RabbitMqMessageBus(MessageBus):

    def __init__(self, host='localhost', port=5672):
        self._subscribers = {}
        self._conn_params = pika.ConnectionParameters(host=host, port=port)
        self._consumers = []
        self._conn = None

    def publish(self, topic_name, message: bytes):
        try:
            self._publish(topic_name, message)
        except (ConnectionClosed, StreamLostError):
            _logger.info("Connection closed: reconnecting to rabbitmq")
            self._publish(topic_name, message)

    def on(self, topic_name, client_group, callback):
        self._subscribers[topic_name] = (client_group, callback)

    def start_consuming(self):
        consumer_count = len(self._subscribers.items())
        if consumer_count == 0:
            return

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

    def _connect(self):
        if not self._conn or self._conn.is_closed:
            self._conn = pika.BlockingConnection(self._conn_params)

    def _publish(self, topic_name, message):
        self._connect()
        channel = self._conn.channel()
        channel.basic_publish(exchange=topic_name, routing_key='', body=message)


class RabbitMqConsumer(multiprocessing.Process):

    def __init__(self, conn_params, topic_name, client_group, callback):
        super().__init__()
        self._conn_params = conn_params
        self._topic_name = topic_name
        self._client_group = client_group
        self._name_generator = RabbitMqQueueNameGenerator(topic_name, client_group)
        self._callback = callback

    def run(self):
        # create new conn
        conn = pika.BlockingConnection(self._conn_params)

        # create channel
        channel = conn.channel()
        self.register_queues(channel)
        channel.basic_qos(prefetch_count=1)

        # start consuming
        threads = []
        wrapped_callback = _callback_wrapper(conn, self._name_generator, self._callback, threads)
        channel.basic_consume(queue=self._name_generator.queue_name(), auto_ack=False,
                              on_message_callback=wrapped_callback)

        _logger.info(
            f"Waiting incoming message for topic: {self._name_generator.exchange_name()}. To exit press Ctrl+C")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

        # Wait for all to complete
        for thread in threads:
            thread.join()

        conn.close()

        _logger.info(f"Consumer stopped")

    def register_queues(self, channel):
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

    @staticmethod
    def message_expired(properties):
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

    def __init__(self, conn, names_gen: RabbitMqQueueNameGenerator, channel: Channel, delivery: Basic.Deliver,
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
        self._conn = conn

    def ack(self):
        def cb():
            self._channel.basic_ack(delivery_tag=self._delivery.delivery_tag)

        self._conn.add_callback_threadsafe(cb)

    def nack(self):
        def cb():
            self._channel.basic_nack(delivery_tag=self._delivery.delivery_tag, requeue=False)

        self._conn.add_callback_threadsafe(cb)

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


def _callback_wrapper(conn, names_gen: RabbitMqQueueNameGenerator, callback, threads):
    """
    Wrapper for callback. since nested function cannot be pickled, we need some top level function to wrap it

    :param callback:
    :return: function
    """

    def fn(ch, method, properties, body):
        if method is None:
            return

        if RabbitMqConsumer.message_expired(properties):
            _logger.debug(f"Max retry reached dropping msg {properties}")
            ch.basic_nack(method.delivery_tag, requeue=False)
            return

        t = threading.Thread(target=callback,
                             args=(RabbitMqConsumerConfirm(conn, names_gen, ch, method, properties, body), body))
        t.start()
        _logger.debug("Thread started")
        threads.append(t)

    return fn
