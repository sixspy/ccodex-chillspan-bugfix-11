"""仓间锁与争用登记（公共并发面）。

锁批门禁（当班封死，只此一路）：
  占锁与入批同处一条写事务，二者择一执行，两边不得混用。
  甲路径（本系统唯一立场）：锁服务拒占 => 正式批次表零增、整笔拒写（HTTP 409）。
  乙路径（影子待锁批次）在本系统不启用：不留待锁/影子行，无晋升环节。
  第三路（无锁仍写正式批次）封死：require_lock 拒占即抛错，事务在提交前中止。
  任何偏离此门禁的写口都按“锁批偏航”处理：记录争用/审计后拒写，绝不落正式批次行。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from auth import AuthHeader, require_user, require_writer
from db import connect, now_iso, write_audit

router = APIRouter(tags=["locks"])


class LockIn(BaseModel):
    lock_key: str
    action: str = "acquire"


def try_acquire(conn, lock_key: str, holder: str) -> bool:
    now = now_iso()
    row = conn.execute(
        "SELECT holder FROM chamber_locks WHERE lock_key=?", (lock_key,)
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO chamber_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
            (lock_key, holder, now),
        )
        return True
    if row["holder"] == holder:
        return True
    conn.execute(
        """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
           VALUES (?,?,?,?,?)""",
        (lock_key, holder, "争用", f"持有者={row['holder']}", now),
    )
    return False


def release(conn, lock_key: str, holder: str) -> None:
    conn.execute(
        "DELETE FROM chamber_locks WHERE lock_key=? AND holder=?",
        (lock_key, holder),
    )


class LockDenied(HTTPException):
    """锁批偏航：占锁被拒即硬闸门，写口必须在提交前中止。"""

    def __init__(self, lock_key: str, holder: str):
        super().__init__(409, f"争用：仓间锁 {lock_key} 已被持有，写路径拒写")
        self.lock_key = lock_key
        self.holder = holder


def require_lock(conn, lock_key: str, holder: str) -> None:
    """硬闸门：占锁失败即抛 LockDenied，正式表零增、整笔拒写。

    拒占时争用留痕与审计在本连接内单独提交（仅日志，无业务行），
    随后抛错使外层写事务随 with/异常回滚，绝不放行无锁写。
    """
    if try_acquire(conn, lock_key, holder):
        return
    write_audit(
        conn,
        holder,
        "lock_gate_deny",
        lock_key,
        {"result": "锁批偏航拒写", "lock_key": lock_key},
    )
    conn.commit()
    raise LockDenied(lock_key, holder)


@router.get("/api/locks")
def list_locks(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM chamber_locks ORDER BY acquired_at DESC"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/contention")
def list_contention(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM contention_log ORDER BY id DESC LIMIT 200"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/locks/acquire")
def acquire_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_writer(authorization)
    with connect() as conn:
        ok = try_acquire(conn, body.lock_key, user)
        write_audit(conn, user, "lock_acquire", body.lock_key, {"ok": ok})
        conn.commit()
        if not ok:
            raise HTTPException(409, "争用：未能获得锁")
        return {"ok": True, "lock_key": body.lock_key, "holder": user}


@router.post("/api/locks/release")
def release_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_writer(authorization)
    with connect() as conn:
        release(conn, body.lock_key, user)
        write_audit(conn, user, "lock_release", body.lock_key, {})
        conn.commit()
        return {"ok": True}