from .tcp_pool import TCP_Connection_Pool

#HOST = "host.docker.internal"
HOST = "127.0.0.1"
PORT = 9000
POOL_SIZE = 32

pool = TCP_Connection_Pool(HOST, PORT, size=POOL_SIZE)
