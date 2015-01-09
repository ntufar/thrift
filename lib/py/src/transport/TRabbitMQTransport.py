import pika
import uuid
import threading
import time
from TTransport import *


class TRabbitMQTransportClient(TTransportBase):
  """RabbitMQ implementation of TTransport base. Use it for clients"""

  def __init__(self, host='localhost', queue='rpc_queue'):
    self.queue = queue
    self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost'))
    self.channel = self.connection.channel()
    self.channel.queue_declare(queue=queue)

    result = self.channel.queue_declare(exclusive=True)
    self.callback_queue = result.method.queue
    self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)
    self.response = None

  def on_response(self, ch, method, props, body):
    if self.corr_id == props.correlation_id:
      self.response = body

  def setHandle(self, h):
    self.handle = h #?

  def isOpen(self):
    return True

  def setTimeout(self, ms):
    pass#?

  def open(self):
    pass

  def readAll(self, sz):
    return read(sz)

  def read(self, sz):
    #print "TRabbitMQTransportClient.read()"
    while self.response is None:
      self.connection.process_data_events()
    return self.response

  def write(self, buff):
    #print "TRabbitMQTransportClient.write()"
    self.response = None
    self.corr_id = str(uuid.uuid4())
    self.channel.basic_publish(exchange='',
                                   routing_key=self.queue,
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=str(buff))

  def flush(self):
    pass

  def listen(self):
    #print "TRabbitMQTransportClient.listen()"
    pass

  def accept(self):
    #print "TRabbitMQTransportClient.accept()"
    while self.response is None:
      self.connection.process_data_events()


#----------------- Server ----------------
class ServerListeningThread(threading.Thread):
  def __init__(self, parent, host, queue):
    threading.Thread.__init__(self)
    self.parent = parent
    self.queue = queue
    self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    self.channel = self.connection.channel()
    self.channel.queue_declare(queue)
    self.channel.basic_qos(prefetch_count=1)
    

  def run(self):
    print "ServerListeningThread.run()"
    self.channel.basic_consume(self.on_data, no_ack=False, queue=self.queue)
    self.channel.start_consuming()
    #print "START CONSUMING FINISHED"

  def on_data(self, ch, method, props, body):
    if self.parent.debug:
      print "TRabbitMQTransportServer.on_data()"
    self.reply_to = props.reply_to
    self.corr_id  = props.correlation_id
    self.parent.data = body
    ch.basic_ack(delivery_tag = method.delivery_tag)
    #print "TRabbitMQTransportServer.on_data() - finished"

  def write(self, buff):
    self.channel.basic_publish(exchange='',
                                   routing_key=self.reply_to,
                                   properties=pika.BasicProperties(
                                         correlation_id = self.corr_id,
                                         ),
                                   body=str(buff))


class TRabbitMQTransportServer(TTransportBase):
  """RabbitMQ implementation of TTransport base. Use it for servers"""

  def __init__(self, host='localhost', queue='rpc_queue', debug=False):
    self.data = None
    self.debug = debug
    self.slt = ServerListeningThread(self, host, queue)
    self.slt.start()
    if self.debug:
      print "TRabbitMQTransportServer.__init__() finished"


  def isOpen(self):
    return True

  def open(self):
    if self.debug:
      print "TRabbitMQTransportServer.open()"
    pass

  def readAll(self, sz):
    return read(sz)

  def read(self, sz):
    if self.debug:
      print "TRabbitMQTransportServer.read()"
    while self.data is None:
      #self.connection.process_data_events()
      time.sleep(0.01)
    #print "TRabbitMQTransportServer.read(): "+ self.data
    d = self.data
    self.data = None
    return d

  def write(self, buff):
    if self.debug:
      print "TRabbitMQTransportServer.write(): "+buff
    self.slt.write(buff)

  def listen(self):
    if self.debug:
      print "TRabbitMQTransportServer.listen()"
    while self.data is None:
      time.sleep(0.01)
      #self.slt.connection.process_data_events()

  def accept(self):
    if self.debug:
      print "TRabbitMQTransportServer.accept()"
    while self.data is None:
      time.sleep(0.01)
      #self.connection.process_data_events()
    return self



