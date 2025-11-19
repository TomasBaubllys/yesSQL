import builder, parser
from fastapi import APIRouter
from tcp_instance_pool import pool

router = APIRouter()

@router.get("/get/{key}")
async def get_value(key: str):
    async with pool.get_connection() as conn:
        msg = builder.build_get_request(key)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed


@router.post("/set/{key}/{value}")
async def set_value(key: str, value: str):
    async with pool.get_connection() as conn:
        msg = builder.build_set_request(key, value)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed


@router.delete("/delete/{key}")
async def remove_value(key: str):
    async with pool.get_connection() as conn:
        msg = builder.build_remove(key)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed
    
@router.post("/create_cursor/{name}/{key}")
async def create_cursor(name: str, key: str):
    async with pool.get_connection() as conn:
        msg = builder.create_cursor(name, key)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed
    

@router.delete("/delete_cursor/{name}")
async def delete_cursor(name: str):
    async with pool.get_connection() as conn:
        msg = builder.delete_cursor(name)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed
    
@router.get("/get_ff/{name}/{amount}")
async def get_ff(name: str, amount: str):
    async with pool.get_connection() as conn:
        msg = builder.get_ff(name, amount)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed

    
@router.get("/get_fb/{name}/{amount}")
async def get_ff(name: str, amount: str):
    async with pool.get_connection() as conn:
        msg = builder.get_fb(name, amount)
        resp = await conn.send(msg)
        parsed = parser.dispatch(resp)
        return parsed
    