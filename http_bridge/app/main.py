
from .tcp_pool import TCP_Connection_Pool
from contextlib import asynccontextmanager
from fastapi import FastAPI
from . import builder, parser

HOST = "127.0.0.1"
PORT = 8080

app = FastAPI()
pool = TCP_Connection_Pool(HOST, PORT, size=5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await pool.initialize()
    print("TCP pool ready!")

    yield
    await pool.close()
    print("TCP pool cleaned")

app = FastAPI(lifespan=lifespan) 
    
@app.get("/get/{key}")
async def get_value(key: str):
    async with pool.get_connection() as conn:
        msg = builder.build_get_request(key)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed


@app.post("/set/{key}/{value}")
async def set_value(key: str, value: str):
    async with pool.get_connection() as conn:
        msg = builder.build_set_request(key, value)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed


@app.delete("/delete/{key}")
async def remove_value(key: str):
    async with pool.get_connection() as conn:
        msg = builder.build_remove(key)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed
    

# TODO:
# COMMAND_CODE_GET_KEYS = 4
#COMMAND_CODE_GET_KEYS_PREFIX = 5
#COMMAND_CODE_GET_FF = 6
#COMMAND_CODE_GET_FB = 7