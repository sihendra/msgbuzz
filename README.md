# Msgbuzz

Generic message bus abstraction. Supported implementations: RabbitMQ

# Build Status
TODO

# Usage

Instantiate `msgbuzz.rabbitmq.RabbitMqMessageBus` to start publishing and consuming.

## Publishing to topic

Publish new message to `profile.new` topic

```python
from msgbuzz.rabbitmq import RabbitMqMessageBus

if __name__ == '__main__':
    msg_bus = RabbitMqMessageBus(host='localhost')

    for i in range(2):
        msg_bus.publish('profile.new', f'Message {i + 1} !!'.encode())


```

## Subscribing to topic

Subscribe for `profile.new` topic and print the message. 

> Note:
>
> Don't forget to ack() or nack() the message


```python
import time

from msgbuzz import ConsumerConfirm
from msgbuzz.rabbitmq import RabbitMqMessageBus

def print_message(op: ConsumerConfirm, message: bytes):
    print(f"{message}")
    time.sleep(2)
    op.ack()


if __name__ == '__main__':
    msg_broker = RabbitMqMessageBus(host='localhost')

    msg_broker.on("profile.new", 'job-norm', print_message)
    
    msg_broker.start_consuming()

```

Please mind that on every subscription `msgbuzz` will span new child process.

# Scaling Subscriber

Increase number of consumers of each subscription to improve performance. 
You do this by:
1. Running the subscriber file (ex: receiver.py in above example) multiple times.
2. Use Supervisord and set numprocs to number larger than one. 

 