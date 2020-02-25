from msgbuzz.rabbitmq import RabbitMqMessageBus

if __name__ == '__main__':
    msg_bus = RabbitMqMessageBus()

    for i in range(2):
        msg_bus.publish('help', f'Unreasonable {i + 1} !!', headers={"header1": "value"})
