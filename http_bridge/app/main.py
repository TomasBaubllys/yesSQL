from .tcp_instance_pool import pool
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .routes import router

HOST = "127.0.0.1"
PORT = 8080
POOL_SIZE = 5

app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await pool.initialize()
    print("TCP pool ready!")

    yield
    await pool.close()
    print("TCP pool cleaned")

app = FastAPI(lifespan=lifespan) 

app.include_router(router)
    

# TODO:
# COMMAND_CODE_GET_KEYS = 4
#COMMAND_CODE_GET_KEYS_PREFIX = 5
#COMMAND_CODE_GET_FF = 6
#COMMAND_CODE_GET_FB = 7