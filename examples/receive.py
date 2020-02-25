import os
import time

from msgbuzz import ConsumerConfirm, Message
from msgbuzz.rabbitmq import RabbitMqMessageBus


def print_message(op: ConsumerConfirm, message: Message):
    print(f"[Process-{os.getpid()}] {message.headers} {message.body}")
    time.sleep(2)
    op.ack()


if __name__ == '__main__':
    msg_broker = RabbitMqMessageBus()

    msg_broker.on("profile.new", 'job-norm', print_message)

    msg_broker.start_consuming()
