__author__ = 'Aviad'
import socket
import SocketServer
import Queue
import sys
import time
import threading
HTTP_PORT = 80
previous_server = 3
lock = threading.Lock()
SERV_HOST = '10.0.0.1'
servers = {'serv1': ['192.168.0.101', None, 0, ('V','P')],  # #video server
 'serv2': ['192.168.0.102', None, 0, ('V','P')],  # #video server
 'serv3': ['192.168.0.103', None, 0, ('M')]}   # #music server

def LBPrint(string):
    print '%s: %s-----' % (time.strftime('%H:%M:%S', time.localtime(time.time())), string)


def createSocket(addr, port):
    for res in socket.getaddrinfo(addr, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, canonname, sa = res
        try:
            new_sock = socket.socket(af, socktype, proto)
        except socket.error as msg:
            LBPrint(msg)
            new_sock = None
            continue

        try:
            new_sock.connect(sa)
        except socket.error as msg:
            LBPrint(msg)
            new_sock.close()
            new_sock = None
            continue

        break

    if new_sock is None:
        LBPrint('could not open socket')
        sys.exit(1)
    return new_sock


def getServerSocket(servID):
    name = 'serv%d' % servID
    return servers[name][1]


def getServerAddr(servID):
    name = 'serv%d' % servID
    return servers[name][0]


def getNextServer(req_type, req_time):
    global lock
    global previous_server
    lock.acquire()
    choosedServerName, new_queue_length = chooseServer(req_type, req_time)
    lock.release()
    return choosedServerName, new_queue_length
def calculateAdditionValue(req_type, req_time, serverType):
    if req_type in serverType:
        return int(req_time)
    elif req_type == 'V':
        return int(req_time)*3
    else:
        return int(req_time)*2
def chooseServer(req_type, req_time):
    global servers
    shortest_server = 'serv1'
    points = calculateAdditionValue(req_type, req_time, servers['serv1'][3])
    shortest_queue = servers['serv1'][2] + points
    for name, [addr, sock, ServerQueueLength, servtype] in servers.iteritems():
        points = calculateAdditionValue(req_type, req_time, servers[name][3])
        if (ServerQueueLength + points) < shortest_queue:
            shortest_server = name
            shortest_queue = ServerQueueLength + points
    return shortest_server,shortest_queue

def parseRequest(req):
    return (req[0], req[1])


class LoadBalancerRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        client_sock = self.request
        req = client_sock.recv(2)
        req_type, req_time = parseRequest(req)
        choosedServerName, new_queue_length = getNextServer(req_type, req_time)
        #LBPrint('recieved request %s from %s, sending to %s' % (req, self.client_address[0], getServerAddr(servID)))
        servers[choosedServerName][2] = new_queue_length
        serv_sock = servers[choosedServerName][1]
        serv_sock.sendall(req)
        data = serv_sock.recv(2)
        client_sock.sendall(data)
        client_sock.close()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


if __name__ == '__main__':
    try:
        for name, (addr, sock, ServerQueueLength, serverType) in servers.iteritems():
            servers[name] = [addr, createSocket(addr, HTTP_PORT), ServerQueueLength, serverType]

        server = ThreadedTCPServer((SERV_HOST, HTTP_PORT), LoadBalancerRequestHandler)
        server.serve_forever()
    except socket.error as msg:
        LBPrint(msg)
