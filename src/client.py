import json
import pika
import traceback

import config

class RPCClient(object):

    def __init__(self, transport, exchange, topic):
        super(RPCClient, self).__init__()
        self.transport = transport
        self.exchange = exchange
        self.topic = topic

    def prepare(self):
        try:
            parameters = pika.URLParameters(self.transport)
            connection = pika.BlockingConnection(parameters)
            self.channel = connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')
        except Exception as ex:
            print('Prepare: %s' % ex)
            traceback.print_exc()

    def send_message(self, message):
        print(message)
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=self.topic,
                                   body=json.dumps(message))


    def __getattr__(self, name):
        def call(*args, **kwargs):
            message = {
                'method': call.__name__,
                'args': args,
                'kwargs': kwargs
            }
            self.send_message(message)
        return call

if __name__ == '__main__':
    client = RPCClient(config.TRANSPORT, config.EXCHANGE, config.TOPIC)
    client.prepare()
    client.add(1, 2)