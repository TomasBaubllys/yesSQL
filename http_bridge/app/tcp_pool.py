import asyncio
from . import constants
import struct
from contextlib import asynccontextmanager


class TCP_Connection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            peer = self.writer.get_extra_info("peername")
            print(f"[TCP] Connected to {self.host}:{self.port} (server peer={peer})")
        except Exception as e:
            print(f"[TCP] FAILED to connect to {self.host}:{self.port}: {e}")
            raise


    async def send(self, data: bytes) -> bytes:
        self.writer.write(data)
        await self.writer.drain()

        try:
            response_length_bytes = await asyncio.wait_for(
                self.reader.readexactly(constants.COMMAND_LENGTH_TOTAL_MESSAGE),
                timeout=2.0
            )
        except asyncio.TimeoutError:
            raise TimeoutError("Timed out waiting for message header")
        except asyncio.IncompleteReadError:
            raise ValueError("Failed to read message header")

        total_length = struct.unpack("!Q", response_length_bytes)[0]

        try:
            response_body = await asyncio.wait_for(
                self.reader.readexactly(total_length - constants.COMMAND_LENGTH_TOTAL_MESSAGE),
                timeout=2.0
            )
        except asyncio.TimeoutError:
            raise TimeoutError("Timed out waiting for message body")
        except asyncio.IncompleteReadError:
            raise ValueError("Failed to read message: message too short")

        return response_length_bytes + response_body

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()


class TCP_Connection_Pool:
    def __init__(self, host, port, size = 10):
        self.host = host
        self.port = port
        self.size = size
        self.pool = asyncio.Queue(size)
        self.semaphore = asyncio.Semaphore(size)

    async def initialize(self):
        for _ in range(self.size):
            conn = TCP_Connection(self.host, self.port)
            await conn.connect()
            await self.pool.put(conn)

    async def acquire(self, timeout=5.0):
        try:
            await asyncio.wait_for(self.semaphore.acquire(), timeout)
        except asyncio.TimeoutError:
            raise TimeoutError("TCP pool exhausted")


        conn = await self.pool.get()

        return conn

    async def release(self, conn: TCP_Connection):
        await self.pool.put(conn)
        self.semaphore.release() 
    
    async def close(self):
        while not self.pool.empty():
            conn = await self.pool.get()
            await conn.close()
            
    @asynccontextmanager
    async def get_connection(self, timeout=5.0):
        conn = await self.acquire(timeout)
        try:
            yield conn
        finally:
            await self.release(conn)