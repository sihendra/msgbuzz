from abc import abstractmethod


class ConsumerConfirm:

    @abstractmethod
    def ack(self):
        pass

    @abstractmethod
    def nack(self):
        pass


class Message(object):

    def __init__(self, headers, body):
        self.headers = headers
        self.body = body


class MessageBus:

    @abstractmethod
    def publish(self, topic_name, message: Message):
        pass

    @abstractmethod
    def on(self, topic_name, client_group, callback):
        pass

    @abstractmethod
    def start_consuming(self):
        pass

