#from .tcp_instance_pool import pool
from tcp_instance_pool import pool

from contextlib import asynccontextmanager
from fastapi import FastAPI
#from .routes import router
from routes import router


app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await pool.start_cleanup_task()
    print("TCP pool ready!")

    yield
    await pool.stop_cleanup_task()
    print("TCP pool cleaned")

app = FastAPI(lifespan=lifespan) 

app.include_router(router)
    
