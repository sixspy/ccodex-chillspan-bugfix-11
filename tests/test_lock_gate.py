"""BUGFIX-11 锁批门禁红测：占锁失败即硬闸门，禁止无锁正式批。

甲：锁服务拒占 => POST /api/batches 整笔拒写，temp_batches 正式表零增。
乙：拒占不得落任何影子/待锁批次；持锁重试成功才允许进清单（不允许乙式待锁晋升混用）。
争用：两人同仓间并发创建，一人持锁一人拒占；拒占方串行重试（待锁释放）后才成功。

立场：采用甲路径（硬闸门拒写），第三路（无锁正式批）不留。
"""
import pytest
from fastapi.testclient import TestClient

from app import app
from db import connect, init_db


@pytest.fixture()
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _auth(c: TestClient, user: str) -> dict:
    tok = c.post(
        "/api/login", json={"username": user, "password": "123456"}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


def _pick_chamber_probe(c: TestClient, h: dict):
    ch = next(
        x
        for x in c.get("/api/chambers", headers=h).json()["items"]
        if x["chamber_code"] == "CK-HN-03"
    )
    probe = c.get(
        f"/api/probes?chamber_id={ch['id']}", headers=h
    ).json()["items"][0]
    return ch, probe


def _count_batches(chamber_id: int) -> int:
    with connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) c FROM temp_batches WHERE chamber_id=?",
            (chamber_id,),
        ).fetchone()["c"]


def _batch_codes(chamber_id: int) -> set:
    with connect() as conn:
        rows = conn.execute(
            "SELECT batch_code FROM temp_batches WHERE chamber_id=?",
            (chamber_id,),
        ).fetchall()
    return {r["batch_code"] for r in rows}


# ---- 复现夹具：锁拒仍插批（修复前 require_lock 静默放行） ----

def test_jia_lock_rejected_zero_formal_batch_row(_db):
    """失败测甲：拒占仍增正式行 —— 修复前红。"""
    c = TestClient(app)
    holder = _auth(c, "admin")
    contender = _auth(c, "ops")
    ch, probe = _pick_chamber_probe(c, holder)
    lock_key = f"chamber:{ch['id']}"

    # admin 先持仓间锁
    assert (
        c.post(
            "/api/locks/acquire", headers=holder, json={"lock_key": lock_key}
        ).status_code
        == 200
    )
    before = _count_batches(ch["id"])
    payload = {
        "chamber_id": ch["id"],
        "probe_id": probe["id"],
        "cargo_ref": "锁拒复现货载-甲",
        "temp_points": [{"t": "08:00", "c": 4.0}],
        "batch_code": "TB-LOCKGATE-JIA",
    }

    r = c.post("/api/batches", headers=contender, json=payload)

    # 断言 1：拒占必须是硬闸门 409，而不是 200 静默放行
    assert r.status_code == 409
    # 断言 2：正式批次表零增
    assert _count_batches(ch["id"]) == before
    # 断言 3：批清单不得出现该无锁批次
    assert "TB-LOCKGATE-JIA" not in _batch_codes(ch["id"])


def test_yi_no_shadow_pending_batch_until_lock_held(_db):
    """失败测乙：待锁影子未晋升却已进清单 —— 甲路径下拒写零痕迹，持锁重试才入清单。"""
    c = TestClient(app)
    holder = _auth(c, "admin")
    contender = _auth(c, "ops")
    ch, probe = _pick_chamber_probe(c, holder)
    lock_key = f"chamber:{ch['id']}"
    assert (
        c.post(
            "/api/locks/acquire", headers=holder, json={"lock_key": lock_key}
        ).status_code
        == 200
    )
    before_codes = _batch_codes(ch["id"])
    payload = {
        "chamber_id": ch["id"],
        "probe_id": probe["id"],
        "cargo_ref": "锁拒复现货载-乙",
        "temp_points": [{"t": "09:00", "c": 3.5}],
        "batch_code": "TB-LOCKGATE-YI",
    }

    blocked = c.post("/api/batches", headers=contender, json=payload)

    # 断言 1：拒占整笔拒写
    assert blocked.status_code == 409
    # 断言 2：正式表零改，且不存在任何待锁/影子行混入清单（无 待锁/影子/pending 状态行）
    assert _batch_codes(ch["id"]) == before_codes
    with connect() as conn:
        shadow = conn.execute(
            """SELECT COUNT(*) c FROM temp_batches
               WHERE batch_code=?
                 AND (status LIKE '%待锁%' OR status LIKE '%影子%'
                      OR status LIKE '%pending%' OR status LIKE '%shadow%')""",
            ("TB-LOCKGATE-YI",),
        ).fetchone()["c"]
    assert shadow == 0

    # 断言 3：持锁者释放后，争用方串行重试成功，批次才原子进入正式清单
    assert (
        c.post(
            "/api/locks/release", headers=holder, json={"lock_key": lock_key}
        ).status_code
        == 200
    )
    retry = c.post("/api/batches", headers=contender, json=payload)
    assert retry.status_code == 200
    assert "TB-LOCKGATE-YI" in _batch_codes(ch["id"])


def test_contention_two_writers_same_chamber_one_lockless_rejected(_db):
    """争用失败测：双人同仓间，一个持锁一个拒占；拒占方无锁不得成功，串行重试须先红。"""
    c = TestClient(app)
    h1 = _auth(c, "admin")
    h2 = _auth(c, "ops")
    ch, probe = _pick_chamber_probe(c, h1)
    lock_key = f"chamber:{ch['id']}"
    base = _count_batches(ch["id"])

    # 第一位写者持锁并完成入批（随后释放，模拟串行窗口）
    first = c.post(
        "/api/batches",
        headers=h1,
        json={
            "chamber_id": ch["id"],
            "probe_id": probe["id"],
            "cargo_ref": "争用货载-持锁方",
            "temp_points": [{"t": "08:00", "c": 4.2}],
            "batch_code": "TB-CONTENTION-A",
        },
    )
    assert first.status_code == 200

    # 持锁方在批结束即释放；此处由 admin 再显式占住同一把锁，制造双人同间争用
    assert (
        c.post(
            "/api/locks/acquire", headers=h1, json={"lock_key": lock_key}
        ).status_code
        == 200
    )
    second = c.post(
        "/api/batches",
        headers=h2,
        json={
            "chamber_id": ch["id"],
            "probe_id": probe["id"],
            "cargo_ref": "争用货载-拒占方",
            "temp_points": [{"t": "09:00", "c": 4.4}],
            "batch_code": "TB-CONTENTION-B",
        },
    )

    # 断言 1：拒占方拿到 409，不能无锁成功
    assert second.status_code == 409
    # 断言 2：只有持锁方的一笔落库，无锁方零增
    assert _count_batches(ch["id"]) == base + 1
    # 断言 3：争用留痕；锁释放后串行重试成功（先红后绿的重试闸门）
    cont = c.get("/api/contention", headers=h1).json()["items"]
    assert any(x["lock_key"] == lock_key for x in cont)
    c.post("/api/locks/release", headers=h1, json={"lock_key": lock_key})
    retry = c.post(
        "/api/batches",
        headers=h2,
        json={
            "chamber_id": ch["id"],
            "probe_id": probe["id"],
            "cargo_ref": "争用货载-拒占方重试",
            "temp_points": [{"t": "10:00", "c": 4.6}],
            "batch_code": "TB-CONTENTION-B",
        },
    )
    assert retry.status_code == 200
    assert _count_batches(ch["id"]) == base + 2


def test_five_views_hide_lockless_formal_batch(_db):
    """五页同源：锁拒批次不得出现在批次/仓间/封批/探头/对账任一读口。"""
    c = TestClient(app)
    holder = _auth(c, "admin")
    contender = _auth(c, "ops")
    ch, probe = _pick_chamber_probe(c, holder)
    lock_key = f"chamber:{ch['id']}"
    c.post("/api/locks/acquire", headers=holder, json={"lock_key": lock_key})
    code = "TB-FIVE-VIEWS-GHOST"
    payload = {
        "chamber_id": ch["id"],
        "probe_id": probe["id"],
        "cargo_ref": "五页同源无锁批",
        "temp_points": [{"t": "08:00", "c": 4.1}],
        "batch_code": code,
    }
    denied = c.post("/api/batches", headers=contender, json=payload)
    assert denied.status_code == 409

    # 1) 批次页：清单无该批
    listed = c.get("/api/batches", headers=holder).json()["items"]
    assert all(b["batch_code"] != code for b in listed)

    # 2) 仓间页：详情 recent_batches 无该批，在批计数不因锁拒而增
    detail = c.get(f"/api/chambers/{ch['id']}", headers=holder).json()
    assert all(b["batch_code"] != code for b in detail["recent_batches"])
    assert detail["active_batch_count"] == _count_batches(ch["id"])

    # 3) 封批页：快照清单无该批关联
    seals = c.get("/api/seals", headers=holder).json()["items"]
    assert all(s["batch_code"] != code for s in seals)

    # 4) 探头页：探头仍在线、未因锁拒派生出任何批次
    pr = c.get(
        f"/api/probes?chamber_id={ch['id']}", headers=holder
    ).json()["items"]
    hit = next(p for p in pr if p["id"] == probe["id"])
    assert hit["online_status"] == "在线"
    assert all(b["batch_code"] != code for b in listed if b["probe_id"] == probe["id"])

    # 5) 对账页：在批计数不含无锁批，配额仍平衡
    recon = c.get(f"/api/reconcile/{ch['chamber_code']}", headers=holder).json()
    assert recon["active_batches"] == _count_batches(ch["id"])
    assert recon["balanced"] is True
