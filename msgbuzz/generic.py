from abc import abstractmethod


class ConsumerConfirm:

    @abstractmethod
    def ack(self):
        pass

    @abstractmethod
    def nack(self):
        pass

    @abstractmethod
    def retry(self, delay, max_retries):
        """
        Retry the message
        :param delay: delay in milliseconds
        :param max_retries: max retry attempt
        :return:
        """
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
