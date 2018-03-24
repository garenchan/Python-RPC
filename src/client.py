import json
import pika
import traceback
import uuid

import config

class RPCClient(object):

    def __init__(self, transport, exchange, topic, result_exchange=None):
        super(RPCClient, self).__init__()
        self.transport = transport
        self.exchange = exchange
        self.topic = topic
        self.result_exchange = result_exchange if result_exchange else self.exchange
        
        # We use a queue per a client to get result.
        # If we get multiple accumulated results, we need to judge which call they are correlated with.
        # So we generate a random uuid to identify a call
        self.correlation_id = None
        self.response = None
        
    def on_response(self, ch, method, props, body):
        if self.correlation_id == props.correlation_id:
            self.response = body

    def prepare(self):
        try:
            parameters = pika.URLParameters(self.transport)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')
            
            # create exchange and queue to get result
            self.channel.exchange_declare(exchange=self.result_exchange, exchange_type='direct')
            result_queue = self.channel.queue_declare(exclusive=True)
            self.result_queue_name = result_queue.method.queue
            self.channel.queue_bind(exchange=self.result_exchange, queue=self.result_queue_name, routing_key=self.result_queue_name)
            self.channel.basic_consume(self.on_response, queue=self.result_queue_name)
        except Exception as ex:
            print('Prepare: %s' % ex)
            traceback.print_exc()

    def send_message(self, message):
        # We use a queue per a client to get result.
        # If we get multiple accumulated results, we need to judge which call they are correlated with.
        # So we generate a random uuid to identify a call
        self.correlation_id = uuid.uuid4().hex
        self.response = None
        self.channel.basic_publish(exchange=self.exchange,
                                   routing_key=self.topic,
                                   properties=pika.BasicProperties(
                                       reply_to = self.result_queue_name,
                                       correlation_id = self.correlation_id
                                   ),
                                   body=json.dumps(message).encode('utf-8'))
        while self.response is None:
            self.connection.process_data_events()
        try:
            response = self.response.decode('utf-8')
            response = json.loads(response)
        except Exception:
            raise Exception('Server Return Unknown Response')
        else:
            status = response.get('status', None)
            if status is None:
                raise Exception('Server Return Unknown Response')
            elif status == 'success':
                return response['data']
            else:
                raise Exception(response['error'])

    def __getattr__(self, name):
        def call(*args, **kwargs):
            message = {
                'method': call.__name__,
                'args': args,
                'kwargs': kwargs
            }
            return self.send_message(message)
        call.__name__ = name
        return call

if __name__ == '__main__':
    client = RPCClient(config.TRANSPORT, config.EXCHANGE, config.TOPIC, config.RESULT_EXCHANGE)
    client.prepare()
    print(client.sub(1, 2))
    print(client.add(1, 2))
    print(client.div(1, 0))