import asyncio
import struct
import time
#from . import constants
import constants


SESSION_IDLE_TIMEOUT = 300
CLEANUP_INTERVAL = 60

class TCP_Connection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader = None
        self.writer: asyncio.StreamWriter = None
        self.lock = asyncio.Lock()
        self.last_used = time.time()
        self.closed = False

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self.last_used = time.time()

    def touch(self):
        self.last_used = time.time()

    async def send(self, data: bytes, timeout=5.0) -> bytes:
        if self.closed:
            raise ConnectionError("TCP connection closed")
        
        self.writer.write(data)
        await self.writer.drain()
        self.touch()

        header = await asyncio.wait_for(self.reader.readexactly(constants.COMMAND_LENGTH_TOTAL_MESSAGE), timeout=timeout)
        total_len = struct.unpack("!Q", header) [0]

        body = await asyncio.wait_for(self.reader.readexactly(total_len - constants.COMMAND_LENGTH_TOTAL_MESSAGE), timeout=timeout)
        self.touch()

        return header + body
    
    async def close(self):
        if not self.closed:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
            self.closed = True



class SessionTCPPool:
    def __init__(self, host, port):
        self.host = host
        self.port = port 
        self._sessions = {}
        self._lock = asyncio.Lock()
        self._cleanup_task = None
        self._stopping = False

    async def start_cleanup_task(self):
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop_cleanup_task(self):
        self._stopping = True
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        async with self._lock:
            sessions = list(self._sessions.items())
            self._sessions.clear()

        for _, conn in sessions:
            await conn.close()

    
    async def _cleanup_loop(self):
        while not self._stopping:
            await asyncio.sleep(CLEANUP_INTERVAL)
            await self.cleanup_idle_sessions()

    async def cleanup_idle_sessions(self):
        now = time.time()
        to_close = []

        async with self._lock:
            for sid, conn in list(self._sessions.items()):
                if now - conn.last_used > SESSION_IDLE_TIMEOUT:
                    to_close.append((sid, conn))
                    del self._sessions[sid]

        for sid, conn in to_close: 
            await conn.close()

    async def get_connection(self, session_id: str):
        async with self._lock:
            conn = self._sessions.get(session_id)
            if conn and not conn.closed:
                conn.touch()
                return conn
            
            conn = TCP_Connection(self.host, self.port)
            await conn.connect()
            self._sessions[session_id] = conn
            return conn
                
    async def send_for_session(self, session_id: str, data: bytes, timeout=5.0) -> bytes:
        conn = await self.get_connection(session_id)
        async with conn.lock:
            return await conn.send(data, timeout=timeout)
        


