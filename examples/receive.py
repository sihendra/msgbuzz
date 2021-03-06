import logging
import os
import time

from msgbuzz import ConsumerConfirm
from msgbuzz.rabbitmq import RabbitMqMessageBus

logging.basicConfig(format='%(asctime)s - %(process)d - %(levelname)s : %(message)s',
                    level=os.getenv('LOG_LEVEL', 'INFO').upper())


def print_message(op: ConsumerConfirm, message: bytes):
    logging.info(message.decode("utf-8"))
    time.sleep(2)
    op.ack()


if __name__ == '__main__':
    msg_broker = RabbitMqMessageBus()

    msg_broker.on("profile.new", 'job-norm', print_message)
    msg_broker.on("profile.complete", 'job-norm', print_message)

    msg_broker.start_consuming()
