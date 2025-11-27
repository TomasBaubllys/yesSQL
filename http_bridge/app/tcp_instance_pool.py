#from .session_tcp_pool import SessionTCPPool

from session_tcp_pool import SessionTCPPool

HOST = "host.docker.internal"
#HOST = "127.0.0.1"
PORT = 9000
POOL_SIZE = 32

pool = SessionTCPPool(HOST, PORT)
