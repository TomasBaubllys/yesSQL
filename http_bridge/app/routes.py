import builder
import parser

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from tcp_instance_pool import pool


router = APIRouter()


# -------------------------------
#  Models
# -------------------------------
class KeyRequest(BaseModel):
    key: str


class SetRequest(BaseModel):
    key: str
    value: str


class CursorRequest(BaseModel):
    name: str
    key: str = "0"


class PrefixRequest(BaseModel):
    name: str
    prefix: str
    amount: int


class FFRequest(BaseModel):
    name: str
    amount: int


# -------------------------------
#  Helpers
# -------------------------------
def require_session(session_id: str):
    if not session_id:
        raise HTTPException(400, detail="Missing X-Session-Id header")
    return session_id


# -------------------------------
#  Endpoints
# -------------------------------

@router.post("/get")
async def get_value(req: KeyRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_get_request(req.key)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, False)


@router.post("/set")
async def set_value(req: SetRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_set_request(req.key, req.value)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, False)


@router.delete("/delete")
async def remove_value(req: KeyRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_remove(req.key)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, False)


@router.post("/create_cursor")
async def create_cursor(req: CursorRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.create_cursor(req.name, req.key)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, False)


@router.delete("/delete_cursor")
async def delete_cursor(req: KeyRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.delete_cursor(req.key)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, False)


@router.post("/get_ff")
async def get_ff(req: FFRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_get_ff(req.name, req.amount)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, False)


@router.post("/get_fb")
async def get_fb(req: FFRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_get_fb(req.name, req.amount)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, False)


@router.post("/get_keys")
async def get_keys(req: FFRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_get_keys(req.name, req.amount)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, True)


@router.post("/get_keys_prefix")
async def get_keys_prefix(req: PrefixRequest, x_session_id: str = Header(None, alias="X-Session-Id")):
    session_id = require_session(x_session_id)
    msg = builder.build_get_keys_prefix(req.name, req.prefix, req.amount)
    resp = await pool.send_for_session(session_id, msg)
    return parser.dispatch(resp, True)
