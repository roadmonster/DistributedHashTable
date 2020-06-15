import socket
import sys
import hashlib
from socket import AF_INET, SOCK_DGRAM


class DhtClient:
    """
    Abstract the field and behavior of dht client into this class.
    This class mainly read input from the commandline and pack it up into formated request then send to UDP socket
    to some dht node and receive response from that socket.
    """
    def __init__(self, name, port, method, key, value):
        """
        Constructor
        :param name: name of the dht node server
        :param port: service port
        :param method: put or get
        :param key: string key
        :param value: value
        """
        self.server_name = name
        self.server_port = port
        self.key = key
        self.value = value
        self.client_name = socket.gethostname()
        self.client_port = "11165"
        self.msg = method + "$" + key + "$" + value + "$" + "1" + "#" + self.client_name + "#" + self.client_port


    def process_request(self):
        """
        This method builds up the socket and pack up the essential info the create the request
        and receive the feedback from the socket and parse then display
        :return:
        """
        client_sock = socket.socket(AF_INET, SOCK_DGRAM)
        client_sock.bind(('', int(self.client_port)))
        client_sock.sendto(self.msg.encode(), (socket.gethostbyname(self.server_name), int(self.server_port)))
        resp_msg, _ = client_sock.recvfrom(2048)
        key_hash = hashlib.sha1(dhtClient.key.encode()).hexdigest()
        nodeid, hops, msg = self.parseInfo(resp_msg)
        client_sock.close()
        print("Key Hash: {}\nNode Hash ID: {}\nNumber of hops: {}\nKey String: {}\nValue String: {}\nResponse Message: {}".format(
            key_hash, nodeid, hops, self.key, self.value, msg
        ))

    def parseInfo(self, msg):
        """
        This method parse out the message received from the dht node
        :param msg: message to be parsed out
        :return: nodeid, hop number, message triple
        """
        msg = msg.decode()
        idx = msg.find('$')
        nodeid = msg[:idx]
        idx1 = msg.find('#')
        hops = msg[idx + 1: idx1]
        msg = msg[idx1 + 1:]
        return nodeid, hops, msg


if __name__ == "__main__":

    if len(sys.argv) < 5 or len(sys.argv) > 6:
        print("invalid arguments\n")
        print("Usage: ./ script_name servername serverPort method key [optional]value ")
        exit(-1)

    sname = sys.argv[1]
    port = sys.argv[2]
    method = sys.argv[3]
    key = sys.argv[4]
    value = ""

    print(sname, port, method, key)
    method_supported = ["get", "put"]
    if method not in method_supported:
        print('Method not supported or key is not valid')
        exit(-1)

    if method.lower() == 'get':
        if len(sys.argv) == 6:
            print('invalid argument, get method only needs to use key')
            exit(-1)

    else:
        if len(sys.argv) == 6:
            value = sys.argv[5]

    dhtClient = DhtClient(sname, port, method, key, value)
    dhtClient.process_request()

