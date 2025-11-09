from .tcp_pool import TCP_Connection_Pool

HOST = "127.0.0.1"
PORT = 8080
POOL_SIZE = 5

pool = TCP_Connection_Pool(HOST, PORT, size=POOL_SIZE)
