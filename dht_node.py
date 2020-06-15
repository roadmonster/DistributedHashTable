"""
File: DHT_node
Hao Li
CPSC5510 Seattle University
"""
import socket
import sys
import os
from socket import AF_INET, SOCK_DGRAM
import hashlib


class BaseNode:
    """
    Lowest level of abstraction to store the address, port and its sha1 hash result for the address and port as node id
    """

    def __init__(self, addr, port):
        """
        Constructor
        :param addr: IP addr
        :param port: Port #
        """
        self.addr = socket.gethostbyname(addr)
        self.port = port
        self.hash_ID = int(hashlib.sha1(socket.inet_pton(AF_INET, self.addr) +
                                        self.port.to_bytes(2, byteorder="big")).hexdigest(), 16)

    def get_info(self):
        return self.addr, self.port, self.hash_ID


class DhtNode:
    """
    Higher level of abstraction to include BaseNode instance for itself, its predecessor, its succesor and the list of
    the nodes in the chord ring
    """

    def __init__(self, myNode, pred, succ, nodeList):
        """
        Constructor
        :param myNode: base node instance for myself
        :param pred: base node instance for my predecessor
        :param succ: base node instance for my successor
        :param nodeList: list of the basenode elements

        """
        self.myNode = myNode
        self.pred = pred
        self.succ = succ

        self.data = {}
        self.totalNodesNum = 2 ** 160
        self.nodeList = nodeList
        self.ft = self.build_ft(myNode)

    def get_node_detail(self, target):
        """
        Utility function to take the string input and return the matching data field of the class
        :param target: abbreviation name of the data filed needed to retreive
        :return: recursively calling the get_info() function in the BaseNode class to get the ip, port and nodeID as a triple
        if the target is not supported, return a triple of -1,-1,-1
        """
        if target == 'pred':
            return self.pred.get_info()
        elif target == 'succ':
            return self.succ.get_info()
        elif target == 'me':
            return self.myNode.get_info()
        else:
            return -1, -1, -1

    def read_data(self, key):
        """
        Read the data from the dictionary object of the class filed
        :param key: string to be used as key to read data
        :return: the value stored matching the key given or -1 if the key does not exist
        """
        if key in self.data:
            return self.data[key]
        else:
            return -1

    def update_data(self, key, val):
        """
        Update the key value pair in the data object of dht node
        :param key: string key
        :param val: value

        """
        self.data[key] = val

    def delete_data(self, key):
        """
        Method to delete the key from the data dictionary
        :param key: string key
        :return: the value matching the key
        """
        return self.data.pop(key)

    def create_data(self, key, val):
        """
        Method to create entry into the data dictionary
        :param key: string key
        :param val: value
        """
        self.data[key] = val

    def build_ft(self, myNode):
        """
        This method calculate and build the finger table
        :param myNode: instance variable of the Node class
        :return: the list of Entry elements
        """
        entries = []

        print('------Populating entries---------')
        for i in range(1, 161):

            st = (myNode.hash_ID + 2 ** (i - 1)) % self.totalNodesNum
            end = (myNode.hash_ID + 2 ** i) % self.totalNodesNum
            m = []
            for node in self.nodeList:
                if node.hash_ID >= st:
                    m.append(node)

            # If the start is bigger than any one of the elements in the nodelist
            # Then its successor is the first one in the ordered nodelist
            if len(m) == 0:
                m.append(self.nodeList[0])

            m.sort(key=lambda x: x.hash_ID)
            mySuccessor = m[0]

            entry = Entry(st, end, mySuccessor)
            entries.append(entry)

        # put all the successor into a set and group entries based on successors
        succSet = set()
        for j in range(0, len(entries) - 1):
            succSet.add(entries[j].successor)

        print("set size: {}".format(len(succSet)))
        for s in succSet:
            print(s.get_info())

        newEntries = []

        for t in succSet:
            temp = []
            for q in entries:
                if q.successor == t:
                    temp.append(q)

            temp.sort(key=lambda x: x.start)

            neoEntry = Entry(temp[0].start, temp[-1].end, t)
            newEntries.append(neoEntry)

        print('---------merge done------------')

        return newEntries

    def lookup_table(self, key):
        """
        This class does the job of looking up the key in the finger table
        if the key falls within the interval of a entry, return its successor
        else return the last entry's successor and it's their problem. Sounds good.
        :param key: string key
        :return: the BaseNode instance for the successor of the predecessor's BaseNode
        """
        key = int(hashlib.sha1(key.encode()).hexdigest(), 16)

        for entry in self.ft:

            end = entry.end
            if entry.end < entry.start:
                end = entry.end + self.totalNodesNum
            if end > key > entry.start:
                print("Found in ft")
                return entry.successor

        print("not found in ft, return the last one in ft")
        # might be problematic because returning the last entry in the fingertable, could result in returning the
        # node with the smallest node id and get a infinite loop
        return self.pred


class Entry:
    """
    Each entry in the finger table is an object
    Use this abstraction to hold all the information in one row
    """

    def __init__(self, start, end, successor):
        """
        Constructor
        :param start: the beginning of my range inclusively
        :param end: the end of the range exclusively
        :param successor: any id that qualifies my range will belong to this successor
        """
        self.start = start
        self.end = end
        self.successor = successor


class Initializer:
    """
    This is the entry point for the whole program, everytime dht_node.py was called, Initializer shall be called.
    This class mainly read the hostfile, turn all the entries into BaseNode objects, and put them into a list, and
    my its own BaseNode
    """
    def __init__(self, fname, ln):
        """
        Constructor to read the text file and parse out the info to create Node objects
        :param fname: host file name
        :param ln: line number
        """
        nodelist = []
        myNode = None
        self.myDhtNode = None
        counter = 1
        if os.path.exists(fname):
            with open(fname, 'r') as f:
                try:
                    for line in f:

                        idx = line.find(" ")
                        hostname = line[:idx]
                        endidx = line.find('\n')
                        idx += 1
                        port = int(line[idx:endidx])
                        node = BaseNode(hostname, port)

                        if counter == ln:
                            myNode = node
                        nodelist.append(node)
                        counter += 1
                except IOError:
                    print("Error in reading file")
                    exit(-1)
                f.close()
        else:
            print("File not exist")
            exit(-1)

        nodelist.sort(key=lambda x: x.hash_ID)

        print('-----------------------------')
        for n in nodelist:
            print(n.get_info())
        print('-----------------------------')
        my_index = nodelist.index(myNode)
        if len(nodelist) - 1 > my_index > 0:
            my_pred = nodelist[my_index - 1]
            my_succ = nodelist[my_index + 1]

        elif my_index == 0:
            my_pred = nodelist[-1]
            my_succ = nodelist[1]
        else:
            my_pred = nodelist[my_index - 1]
            my_succ = nodelist[0]

        self.myDhtNode = DhtNode(myNode, my_pred, my_succ, nodelist)

    def begin_serving(self):
        """
        This is the wrapper
        :return:
        """
        server = Server(self.myDhtNode)
        server.stand_by()


def parse_info(request):
    """
    This method parse out the info from the request
    :param request:
    :return:
    """
    dict = {}
    idx1 = request.find("$")
    idx2 = request.rfind("$")
    idx3 = request.find("#")
    idx4 = request.rfind("#")

    method = request[:idx1].lower()

    key_val = request[idx1 + 1: idx2]

    key = key_val[:key_val.find("$")]

    val = key_val[key_val.find("$") + 1:]

    hop_num = int(request[idx2 + 1: idx3])

    originAddr = request[idx3 + 1: idx4]

    originPort = request[idx4 + 1:]

    dict['method'] = method
    dict['key'] = key
    dict['val'] = val
    dict['hop'] = str(hop_num + 1)
    dict['originAddr'] = originAddr
    dict['originPort'] = originPort

    return dict


class Server:
    """
    Node class for storing, checking, responding, and routing request from the client
    """

    def __init__(self, dht_node):
        """
        Constructor
        :param dht_node: instance for DHT_Node

        """
        self.dht_node = dht_node
        self.server_socket = socket.socket(AF_INET, SOCK_DGRAM)
        self.server_socket.bind(('', self.dht_node.myNode.port))

        print("Ready to receive RPCs. \n")

    def stand_by(self):
        """
        Receive UDP packet from the socket and parse out the information including method, key and value
        UDP packet shall have the content in the format:
        method$key$value$hop_num#original_clientAddr#port

        """

        while True:
            request, clientAddress = self.server_socket.recvfrom(2048)
            print("Received packet\n")
            request = request.decode()

            dictionary = parse_info(request)

            clientSocket = socket.socket(AF_INET, SOCK_DGRAM)

            method = dictionary['method']
            originAddr = socket.gethostbyname(dictionary['originAddr'])
            originPort = int(dictionary['originPort'])
            key = dictionary['key']

            value = dictionary['val']
            hops = dictionary['hop']

            print("key: {} value: {}".format(key, value))

            # use conduct function to get where to do next, if it is 'me' then retrieve info locally
            next_step = self.conduct(key)

            if next_step == 'me':
                val_read = self.dht_node.read_data(key)
                # perform get operation
                if method == 'get':
                    # No record found
                    if val_read == -1:
                        response_msg = self.compose_response('Error: no matching result.', hops)

                    # Found record, send the record back
                    else:
                        response_msg = self.compose_response(str(val_read), hops)

                # perform put operation
                else:
                    # Record exist with given key
                    if val_read != -1:
                        # length of val is 0 then delete
                        if len(dictionary['val']) == 0:  # may be problematic
                            self.dht_node.delete_data(key)
                            delete_success = "Record {} successfully deleted".format(key)
                            response_msg = self.compose_response(delete_success, hops)

                        # put with a value with key already exists, then update
                        else:
                            update_success_msg = "Key: {}, Value: {} successfully updated".format(key, value)
                            self.dht_node.update_data(key, value)
                            response_msg = self.compose_response(update_success_msg, hops)

                    # Record not exist for the given key
                    else:
                        # length of val is not 0 then create
                        if len(value) != 0:
                            create_success_msg = "Key: {} Value: {} successfully created".format(key, value)
                            self.dht_node.create_data(key, value)
                            response_msg = self.compose_response(create_success_msg, hops)
                        else:
                            # length of val given by client is 0, then send error msg back to origin
                            err_msg = "No content to create for the key given"
                            response_msg = self.compose_response(err_msg, hops)

                # Contact the requesting client directly
                clientSocket.sendto(response_msg.encode(), (originAddr, originPort))
                continue

            elif next_step == 'ft':
                target = self.dht_node.lookup_table(key)
                targetAddr, targetPort, _ = target.get_info()
            else:
                targetAddr, targetPort, _ = self.dht_node.get_node_detail(next_step)

            updated_request = self.compose_request(dictionary)
            clientSocket.sendto(updated_request.encode(), (targetAddr, targetPort))

            print("Request diverted to other nodes\n")

    def compose_request(self, db):
        request = db['method'] + '$' + db['key'] + '$' + db['val'] + '$' + db['hop'] + '#' + db['originAddr'] \
                  + '#' + db['originPort']
        return request

    def compose_response(self, msg, hops):
        nodeid = self.dht_node.myNode.hash_ID
        res = str(nodeid) + "$" + hops + "#" + msg
        return res

    def conduct(self, key):
        """
        This method works like the main terminal in the outpatient building. If checks the key and decide which next step
        it will go
        :param key: string key
        :return: string abbreviation for the next step, me as my node, pred as my predecessor, succ as my successor, ft
        stands for finger table
        """
        # turn the key from string of hexadecimals to integer
        hash_key = int(hashlib.sha1(key.encode()).hexdigest(), 16)

        pred_id = self.dht_node.pred.hash_ID
        succ_id = self.dht_node.succ.hash_ID
        my_id = self.dht_node.myNode.hash_ID

        res = None

        if pred_id > succ_id > my_id:
            if hash_key > pred_id or hash_key < my_id:
                res = 'me'
            if succ_id >= hash_key > my_id:
                res = 'succ'
            if hash_key > succ_id:
                res = 'ft'

        elif my_id > pred_id > succ_id:
            if pred_id < hash_key < my_id:
                res = 'me'
            elif hash_key > my_id or hash_key <= succ_id:
                res = 'succ'
            elif hash_key > succ_id:
                res = 'ft'

        elif succ_id > my_id > pred_id:
            if pred_id < hash_key <= my_id:
                res = 'me'
            if my_id < hash_key <= succ_id:
                res = 'succ'
            if hash_key > succ_id:
                res = 'ft'
        return res


if __name__ == "__main__":

    """if len(sys.argv) != 3:
        print('Must at least have two argument\n')
        print('Usage: ./ scriptname filename line_number\n')
    else:
        filename = sys.argv[1]
        lineum = sys.argv[2]
        print('filename: {}, line number: {}'.format(filename, lineum))
        main_logic = Initializer(filename, int(lineum))
        main_logic.begin_serving()"""

    req = "put$washting$olympia$1#cs1.seattleu.edu#11170"
    print(req)
    print(parse_info(req))

