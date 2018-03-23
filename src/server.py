import socket
import uuid
import traceback
import json

import pika

import config


class Endpoint(object):

    def add(self, x, y):
        return x + y

    def sub(self, x, y):
        return x - y


class RPCServer(object):

    def __init__(self, transport, exchange, topic, endpoints=None):
        super(RPCServer, self).__init__()
        self.transport = transport
        self.exchange = exchange
        self.topic = topic
        self.endpoints = endpoints

    def prepare(self):
        try:
            parameters = pika.URLParameters(self.transport)
            connection = pika.BlockingConnection(parameters)
            self.channel = connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')

            self.queue_name = 'rpc-%s-%s' % (socket.gethostname(), uuid.uuid4().hex)
            self.queue = self.channel.queue_declare(queue=self.queue_name, exclusive=False)
            self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.topic)
        except Exception as ex:
            print('Prepare: %s' % ex)
            traceback.print_exc()

    def callback(self, ch, method, properties, body):
        try:
            message = json.load(body)
            print(message)
            method = message.get('method')
        except Exception as ex:
            print(ex)


    def start(self):
        self.channel.basic_consume(self.callback, queue=self.queue_name, no_ack=True)
        self.channel.start_consuming()



if __name__ == '__main__':
    server = RPCServer(config.TRANSPORT, config.EXCHANGE, config.TOPIC)
    server.prepare()
    server.start()