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


class MessageBus:

    @abstractmethod
    def publish(self, topic_name, message: bytes):
        pass

    @abstractmethod
    def on(self, topic_name, client_group, callback):
        pass

    @abstractmethod
    def start_consuming(self):
        pass
