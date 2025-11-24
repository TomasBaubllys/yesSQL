from . import builder, parser
from fastapi import APIRouter, Header, HTTPException
from .tcp_instance_pool import pool

router = APIRouter()

def require_session(session_id: str):
    if not session_id:
        raise HTTPException(400, detail="Missing X-Session-Id header")
    return session_id


@router.get("/get/{key}")
async def get_value(key: str, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_get_request(key)
    resp = await pool.send_for_session(session_id, msg)
    parsed = parser.dispatch(resp)
    return parsed


@router.post("/set/{key}/{value}")
async def set_value(key: str, value: str,  x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_set_request(key, value)
    resp = await pool.send_for_session(session_id, msg)
    parsed = parser.dispatch(resp)
    return parsed


@router.delete("/delete/{key}")
async def remove_value(key: str, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_remove(key)
    resp = await pool.send_for_session(session_id, msg)
    parsed = parser.dispatch(resp)
    return parsed
    
@router.post("/create_cursor/{name}/{key}")
async def create_cursor(name: str, key: str, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.create_cursor(name, key)
    resp = await pool.send_for_session(session_id, msg)
    parsed = parser.dispatch(resp)
    return parsed
    

@router.delete("/delete_cursor/{name}")
async def delete_cursor(name: str, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.delete_cursor(name)
    resp = await pool.send_for_session(session_id, msg)
    parsed = parser.dispatch(resp)
    return parsed
    
@router.get("/get_ff/{name}/{amount}")
async def get_ff(name: str, amount: str, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.get_ff(name, amount)
    resp = await pool.send_for_session(session_id, msg)
    parsed = parser.dispatch(resp)
    return parsed

    
@router.get("/get_fb/{name}/{amount}")
async def get_ff(name: str, amount: str, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.get_fb(name, amount)
    resp = await pool.send_for_session(session_id, msg)
    parsed = parser.dispatch(resp)
    return parsed
