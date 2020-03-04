from msgbuzz import Message
from msgbuzz.rabbitmq import RabbitMqMessageBus

if __name__ == '__main__':
    msg_bus = RabbitMqMessageBus()

    for i in range(2):
        msg_bus.publish('profile.new', Message({"header-1": "value-1"}, f'Message {i + 1} !!'))

    for i in range(2):
        msg_bus.publish('profile.complete', Message({"header-1": "value-2"}, f'Message {i + 1} !!'))
