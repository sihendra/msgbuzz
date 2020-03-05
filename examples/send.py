from msgbuzz.rabbitmq import RabbitMqMessageBus

if __name__ == '__main__':
    msg_bus = RabbitMqMessageBus()

    for i in range(2):
        msg_bus.publish('profile.new', f'Message {i + 1} !!'.encode('utf-8'))

    for i in range(2):
        msg_bus.publish('profile.complete', f'Message {i + 1} !!'.encode('utf-8'))
