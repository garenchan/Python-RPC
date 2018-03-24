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

    def __init__(self, transport, exchange, topic, endpoints=[]):
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
            self.queue = self.channel.queue_declare(queue=self.queue_name, exclusive=True)
            self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.topic)
        except Exception as ex:
            print('Prepare: %s' % ex)
            traceback.print_exc()

    def callback(self, ch, method, properties, body):
        try:
            body = body.decode('utf-8')
            message = json.loads(body)
           
            rpc_method = message.get('method')
            if not rpc_method:
                return
            args = message.get('args', [])
            kwargs = message.get('kwargs', {})
            for endpoint in self.endpoints:
                _method = getattr(endpoint, rpc_method, None)
                if _method and callable(_method):
                    try:
                        ret = _method(*args, **kwargs)
                    except Exception as ex:
                        print(ex)
                    else:
                        print(ret)
                    break
        except Exception as ex:
            print(ex)
        finally:
            ch.basic_ack(delivery_tag = method.delivery_tag)



    def start(self):
        self.channel.basic_consume(self.callback, queue=self.queue_name)
        self.channel.start_consuming()



if __name__ == '__main__':
    server = RPCServer(config.TRANSPORT, config.EXCHANGE, config.TOPIC, [Endpoint()])
    server.prepare()
    server.start()