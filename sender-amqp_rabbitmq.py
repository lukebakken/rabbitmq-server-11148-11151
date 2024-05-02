import optparse
import sys
from time import sleep
import proton
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
from proton import symbol
from proton.reactor import SenderOption
from datetime import datetime
from datetime import timedelta

#other sample with CapabilityOptions
# https://github.com/amqphub/equipage/tree/main/qpid-proton-python/auto-create
class CapabilityOptions(SenderOption):
    def apply(self, sender):
        sender.target.capabilities.put_object(symbol("queue"))

class Send(MessagingHandler):
    def __init__(self, url, messages, sender_name, pause_time):
        super(Send, self).__init__()
        self.url = url
        self.sent = 0
        self.confirmed = 0
        self.total = messages
        self.sender_name = sender_name
        self.pause_time = pause_time

    #This indicates that the event loop in the container has started, and that a new sender and/or receiver may now be created
    # just an alias for on_reactor_init(self, event)
    def on_start(self, event):
        print("   -I- event loop in the container has started")

        cur_url = (self.url).rsplit("/",1)[0]
        cur_address = "/amq/queue/"+ (self.url).rsplit("/",1)[1]
        #cur_address = (self.url).rsplit("/",1)[1]
        print("   -I- trying connection "+ self.url)
        print("   -I-   url="+ cur_url)
        print("   -I-   address="+ cur_address)

        cur_conn = event.container.connect(url=cur_url, user="test", password="test" )
        print(cur_conn)
        print(cur_conn.url) #la connection n'est pas ouverte!
        cur_sender = event.container.create_sender(context=cur_conn, target=cur_address , name=self.sender_name)   #target for sender
        #cur_sender = event.container.create_sender(context=cur_conn, target=cur_address , name=self.sender_name, options=CapabilityOptions() )   #target for sender

        print("   -I- Sender", cur_sender.name, "is created")
        print("   -I-  Max msg size (0 is unlimited)= ", cur_sender.max_message_size)

    def on_connection_opened(self, event):
        print("   -I- Connection", event.connection, "is open")
        print("   -I- connection use url", event.connection.url)

    def on_link_opened(self, event):
        print("   -I- SEND: sender link opened", event.sender, " for target address '{0}'".format
              (event.sender.target.address))

    #handler added for debuging
    def on_connection_closed(self, event):
        print("   -I- Connection", event.connection, "is closed")

    def on_connection_closing(self, event):
        print("   -I- Closing connection", event.connection)

    def on_connection_error(self, event):
        print("   -E- Error on connection", event.connection)
        print("  connection error:", event.connection.condition)
        print(event.transport.authenticated) #event.transport.require_auth(True)
        print("Transport failure for amqp broker:", self.url, "Error:", event.transport.condition)
        MessagingHandler.on_connection_error(self, event)

    def on_link_closed(self,event):
        print("   -I- Link sender is closed for target address '{0}'".format
              (event.sender.target.address))

    def on_link_closing(self,event):
        print("   -I- Closing link sender for target address '{0}'".format
              (event.sender.target.address))

    def on_link_error(self,event):
        print("   -E- Error link sender for target address '{0}'".format
              (event.sender.target.address))
        print(event.transport.condition)

    def on_session_closed(self,event):
        print("   -I- Session is closed")

    def on_session_closing(self,event):
        print("   -I- Session is closing")

    def on_session_error(self,event):
        print("   -E- Session closed with error")

    def on_transport_error(self,event):
        print("   -E- Transport error in the AMQP connection. This includes authentication errors as well as socket errors.")
        print("  Transport error:", event.transport.condition)
        MessagingHandler.on_transport_error(self, event)

    #This callback indicates that send credit has now been set by the receiver, and that a message may now be sent.
    def on_sendable(self, event):  #max 60s timeout to send messages correspomding to credits available
        print("   -I- Credits=", event.sender.credit)
        #event.sender.offered(40) => prefetch in MessagingHandler
        while event.sender.credit and self.sent < self.total:
            #msg = Message(id=(self.sent + 1), body={'sequence': (self.sent + 1)}, properties={'producer': self.sender_name, "_AMQ_DUPL_ID": str(self.sent + 1)},
            msg = Message(id=(self.sent + 1), body={'sequence': (self.sent + 1)}, properties={'producer': self.sender_name},
                        durable=True,
                        creation_time=int( datetime.now().timestamp()),
                        user_id = bytes("batch-sylvain", 'ascii', errors = 'strict'),
                        subject = "test message",
                        content_type = "application/json"
#                        ttl = 200, # in secondes   #use ttl instead of expiry_time
#                        subject = "test message",
                        )
            cid_delivery = event.sender.send(msg) #is an asynchronous call,. The return of the call does not indicate that the messages has been transferred yet.
            print ("Send msg id="+str(self.sent + 1)+"    Delivery"+str(cid_delivery))
            self.sent += 1

            if self.pause_time > 0:
                sleep(self.pause_time)

    #This callback indicates that a message has been received and accepted by the receiving peer(the broker).
    def on_accepted(self, event):
        print("   -I- Delivery", event.delivery, "is accepted by broker")
        #print(event.delivery.settled) # True: the delivery has been settled by the remote peer

        self.confirmed += 1
        if self.confirmed == self.total:
            print("all messages confirmed")
            event.sender.close()
            event.connection.close()

    def on_rejected(self, event):
        print("   -W- Delivery", event.delivery, "is rejected")

    def on_disconnected(self, event):
        self.sent = self.confirmed
        print("   -I- on_disconnected, socket is disconneted")

parser = optparse.OptionParser(usage="usage: %prog [options]",
                               description="Send messages to the supplied address.")
parser.add_option("-a", "--address", default="localhost:5672/exampleQueue",
                  help="address to which messages are sent (default %default)")
parser.add_option("-m", "--messages", type="int", default=10,
                  help="number of messages to send (default %default)")
parser.add_option("-n", "--name", default="sender1",
                  help="name of the Sender link (default %default)")
parser.add_option("-w", "--wait", type="int", default=0, dest="pause_time",
                  help="pause time between sending messages in s (default %default not wait)")
opts, args = parser.parse_args()

try:
    #Container(Send(opts.address, opts.messages)).run()
    container = Container(Send(opts.address, opts.messages, opts.name, opts.pause_time))
    #nom du Client 'Client ID' de la connexion  .If you not set the ID, the library will generate a UUID when the container is constucted.
    container.container_id = "Client Python QIP send - sylvain"
    print ("Starting client sender'"+ container.container_id+ "' with link sender", opts.name, " sending ", opts.messages, "messages")
    if opts.pause_time > 0:
        print("   -I- pause between message, set to", opts.pause_time,"s")
    container.run()
except KeyboardInterrupt:
    pass

