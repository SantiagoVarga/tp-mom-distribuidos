import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True)
        
    def send(self, message):
        if isinstance(message, str):
            message = message.encode()
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)  
        )

    def receive(self):
        # Usa basic_get para obtener un mensaje de la cola sin bloquear
        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name, auto_ack=True)
        if method_frame:
            return body
        else:
            return None

    def start_consuming(self, callback):
        self._consuming = True
        def on_message(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag)
            callback(body, ack, nack)

            if not self._consuming:
                channel.stop_consuming()
        self.consumer_tag = self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )
        try:
            self.channel.start_consuming()
        except Exception:
            pass


    def stop_consuming(self):
        # Señal para terminar el loop de consumo
        self._consuming = False
        try:
            self.channel.stop_consuming()
        except Exception:
            pass

    def close(self):
        try:
            if hasattr(self, 'channel') and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass
        try:
            if hasattr(self, 'connection') and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic', durable=True)
        
        # Declarar una cola anónima para recibir mensajes
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        
        # Vincular la cola a cada routing key
        if isinstance(self.routing_keys, list):
            for key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=key)
        else:
            self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.routing_keys)

    def send(self, message):
        # Usar la primera routing_key de la instancia
        if isinstance(self.routing_keys, list):
            routing_key = self.routing_keys[0]
        else:
            routing_key = self.routing_keys
        # Asegurar que el mensaje es bytes
        if isinstance(message, str):
            message = message.encode()
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def receive(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name, auto_ack=True)
        if method_frame:
            return body, method_frame.routing_key
        return None


    def start_consuming(self, callback):
        self._consuming = True
        def on_message(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)
            def nack():
                channel.basic_nack(delivery_tag=method.delivery_tag)
            callback(body, ack, nack) if callback.__code__.co_argcount == 3 else callback(body, method.routing_key, ack, nack)
            if not self._consuming:
                channel.stop_consuming()
        self.consumer_tag = self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=on_message,
            auto_ack=False
        )
        try:
            self.channel.start_consuming()
        except Exception:
            pass
    
    def stop_consuming(self):
        self._consuming = False
        try:
            self.channel.stop_consuming()
        except Exception:
            pass

    def close(self):
        try:
            if hasattr(self, 'channel') and self.channel.is_open:
                self.channel.close()
        except Exception:
            pass
        try:
            if hasattr(self, 'connection') and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass