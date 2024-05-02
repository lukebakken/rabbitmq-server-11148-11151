import optparse
from time import sleep
from proton.handlers import MessagingHandler
from proton.reactor import Container, Copy
from proton import symbol
from proton.reactor import ReceiverOption

DEBUG="OFF"

def is_integer(n):
    try:
        float(n)
    except ValueError:
        return False
    else:
        return float(n).is_integer()

class CapabilityOptions(ReceiverOption):
    def apply(self, receiver):
        receiver.source.capabilities.put_object(symbol("queue"))

class Recv(MessagingHandler):
    def __init__(self, url, count, receiver_name, pause_time):
        super(Recv, self).__init__(prefetch=10, auto_accept=False)
                                                   #default param: prefetch=10, auto_accept=True, auto_settle=True
                                                   #credit=prefetch value for the receiver
        self.url = url
        self.expected = count
        self.received = 0
        self.receiver_name = receiver_name
        self.maxid = 0 # store event.message.id
        self.pause_time = pause_time

    #This indicates that the event loop in the container has started, and that a new sender and/or receiver may now be created
    def on_start(self, event):
        print("   -I- event loop in the container has started")

        cur_url = (self.url).rsplit("/",1)[0]
        cur_address = (self.url).rsplit("/",1)[1]
        print("   -I- trying connection "+ self.url)
        print("   -I-   url="+ cur_url)
        print("   -I-   address="+ cur_address)

        cur_conn = event.container.connect(url=cur_url, user="test", password="test" )

        print(cur_conn)
        print(cur_conn.url) #la connection n'est pas ouverte!

        cur_receiver = event.container.create_receiver(context=cur_conn, source=cur_address, name=self.receiver_name)  #source for receiver
        #cur_receiver = event.container.create_receiver(context=cur_conn, source=cur_address, name=self.receiver_name, options=CapabilityOptions() )  #source for receiver

        print("   -I- Receiver", cur_receiver.name, "is created")

    def on_connection_opened(self, event):
        print("   -I- Connection", event.connection, "is open")

    def on_link_opened(self, event):
        print("   -I- RECEIVE: receiver link opened ", event.receiver, " for source address '{0}'".format
              (event.receiver.source.address))

    #handler added for debuging
    def on_connection_closed(self, event):
        print("   -I- Connection", event.connection, "is closed")

    def on_connection_closing(self, event):
        print("   -I- Closing connection", event.connection)

    def on_connection_error(self, event):
        print("   -E- Error on connection", event.connection)

    def on_link_closed(self,event):
        print("   -I- Link receiver is closed")

    def on_link_closing(self,event):
        print("   -I- Closing link receiver for target address '{0}'".format
              (event.receiver.target.address))

    def on_link_error(self,event):
        print("   -E- Error link receiver for target")

    def on_disconnected(self,event):
        print("   -I- Socket is disconnected")

    def on_session_closed(self,event):
        print("   -I- Session is closed")

    def on_session_closing(self,event):
        print("   -I- Session is closing")

    def on_session_error(self,event):
        print("   -E- Session closed with error")

    def on_transport_error(self,event):
        print("   -E- Transport error in the AMQP connection. This includes authentication errors as well as socket errors.")

    #This callback indicates that a message has been received. The message and its delivery object may
    #be retreived, and if needed, the message can be either accepted or rejected.
    def on_message(self, event):
        print("   -D- credit=", event.receiver.credit,"  queued (deliveries on this link)=", event.receiver.queued)  #event.receiver.drained
        #print(event.message).
        if DEBUG == "ON":
            print("   -D- Received message =>", event.message, "from", event.receiver)
            print("   -D-    self.received", self.received, "on self.expected", self.expected)
            print("   -D-    self.maxid="+ str(self.maxid) )
            print("   -D-    event.message.correlation_id=", event.message.correlation_id)

        if self.expected == 0 or self.received < self.expected:
            print(event.message.body)

            #if MessagingHandler auto_accept=False, we need to explicit ACCEPTED or REJECTED delivery.
            event.delivery.update(event.delivery.ACCEPTED)  #auto_accept=False  by defaut auto ACCEPTED after return on on_message() event handler.
            #event.delivery.update(event.delivery.REJECTED)  # if rejected, message is not removed from the broker queue.
            event.delivery.settle()

            self.received += 1
            if self.received == self.expected:
                print("   -I- expected=", self.expected)
                event.receiver.close()
                event.connection.close()
            if self.pause_time > 0:
                sleep(self.pause_time)

        # by default the delivery is ACCEPTED after return on handler on_message()
        return

parser = optparse.OptionParser(usage="usage: %prog [options]")
parser.add_option("-a", "--address", default="localhost:5672/exampleQueue",
                  help="address from which messages are received (default %default)")
parser.add_option("-m", "--messages", type="int", default=10,
                  help="number of messages to receive; 0 receives indefinitely (default %default)")
parser.add_option("-n", "--name", default="receiver1",
                  help="name of the Sender link (default %default)")
parser.add_option("-w", "--wait", type="int", default=0, dest="pause_time",
                  help="pause time between receiving messages in s (default %default not wait)")
opts, args = parser.parse_args()

try:
    #Container(Recv(opts.address, opts.messages)).run()
    container = Container(Recv(opts.address, opts.messages, opts.name, opts.pause_time))
    container.container_id = "Client Python QIP recv - sylvain"
    print ("Starting client receiver '"+ container.container_id +"' with link receiver", opts.name, " waiting ", opts.messages, "messages")
    if opts.pause_time > 0:
        print("   -I- pause between message, set to", opts.pause_time,"s")
    container.run()
except KeyboardInterrupt:
    pass

