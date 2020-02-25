from msgbuzz.rabbitmq import RabbitMqMessageBus

if __name__ == '__main__':
    msg_bus = RabbitMqMessageBus()

    for i in range(2):
        msg_bus.publish('profile.new', f'Profile New {i + 1} !!', headers={"header1": "value"})

    for i in range(2):
        msg_bus.publish('profile.complete', f'Profile Complete {i + 1} !!', headers={"header1": "value"})
