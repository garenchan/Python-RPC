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
        
    def div(self, x, y):
        return x / y


class RPCServer(object):

    def __init__(self, transport, exchange, topic, endpoints=[], result_exchange=None):
        super(RPCServer, self).__init__()
        self.transport = transport
        self.exchange = exchange
        self.topic = topic
        self.endpoints = endpoints
        self.result_exchange = result_exchange if result_exchange else self.exchange

    def prepare(self):
        try:
            parameters = pika.URLParameters(self.transport)
            connection = pika.BlockingConnection(parameters)
            self.channel = connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')

            self.queue_name = 'rpc-%s-%s' % (socket.gethostname(), uuid.uuid4().hex)
            self.queue = self.channel.queue_declare(queue=self.queue_name, exclusive=True)
            self.channel.queue_bind(exchange=self.exchange, queue=self.queue_name, routing_key=self.topic)
            # use to return result
            self.channel.exchange_declare(exchange=self.result_exchange, exchange_type='direct')
        except Exception as ex:
            print('Prepare: %s' % ex)
            traceback.print_exc()

    def callback(self, ch, method, props, body):
        response = {
            'status': 'error',
            'data': None,
            'error': 'Method Not Support'
        }
        try:
            body = body.decode('utf-8')
            message = json.loads(body)
           
            rpc_method = message.get('method')
            if not rpc_method:
                response['error'] = 'Method cannot be empty'
                return
            args = message.get('args', [])
            kwargs = message.get('kwargs', {})
            for endpoint in self.endpoints:
                _method = getattr(endpoint, rpc_method, None)
                if _method and callable(_method):
                    try:
                        response['data'] = _method(*args, **kwargs)
                    except Exception as ex:
                        response['error'] = str(ex)
                    else:
                        response['status'] = 'success'
                    break
        except Exception as ex:
            response['error'] = 'Internal Server Error'
        finally:
            if props.reply_to:
                ch.basic_publish(exchange=self.result_exchange,
                                 routing_key=props.reply_to,
                                 properties=pika.BasicProperties(
                                     correlation_id=props.correlation_id),
                                 body=json.dumps(response).encode('utf-8'))
            ch.basic_ack(delivery_tag = method.delivery_tag)


    def start(self):
        self.channel.basic_consume(self.callback, queue=self.queue_name)
        self.channel.start_consuming()



if __name__ == '__main__':
    server = RPCServer(config.TRANSPORT, config.EXCHANGE, config.TOPIC, [Endpoint()], config.RESULT_EXCHANGE)
    server.prepare()
    server.start()