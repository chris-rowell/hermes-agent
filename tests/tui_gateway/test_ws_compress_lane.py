"""A pending manual compression must not hold the per-connection serial lane (#159239, #145737).

Desktop's ``/compress`` travels as canonical ``session.mutate`` with operation ``compress`` and the
gateway awaits the whole summary call. While it runs, every other RPC on the same socket used to wait
behind it: the legacy liveness ``ping`` (#145737), ``session.resume`` (opening another chat),
``session.control.read`` (the composer's controls) and edits of other chats, until the Desktop's 30 s
deadline.

The real ``handle_ws``, ``AuthorityConnection.dispatch``, contract checks and the real handlers for
``ping``, ``session.control.read``, ``session.resume`` and ``prompt.submit`` run unchanged. Only
``session_mutations.mutate_session`` is a gated in-memory stand-in (no provider, no database), and the
fake authority answers the two persistence/authorization calls the resume handler makes for a session
it does not hold, so the reads answer with the real handlers' ``not_found`` refusal and the send with
``permission_denied`` (this socket never attached that chat). Those verdicts are not the point: the
point is that a reply arrives while the compression is still pending.

Ordering kept on the socket: a compression leaves the lane in a chain of its own session, and every
later frame naming that session (a mutation, a send, an interrupt, a resume; a few reads excepted)
follows it in arrival order. Mutations of other sessions stay on the serial lane and answer while
the compression is pending; a compression of another session starts that session's own chain.
"""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway import session_mutations
from persistence.core.errors import AuthorityRefusal
from tui_gateway import server
from tui_gateway import ws as ws_mod

ORIGINAL_MUTATE = session_mutations.mutate_session
ORIGINAL_DISPATCH = ws_mod._dispatch_request


class MemorySocket:
    def __init__(self, authority):
        self.scope = {"hermes.session_authority": authority}
        self.inbound = asyncio.Queue()
        self.frames = []
        self.replies = {}
        self.reply_order = []
        self.reply_events = {}
        self.closed = False

    def reply_event(self, request_id):
        return self.reply_events.setdefault(request_id, asyncio.Event())

    async def accept(self):
        pass

    async def send_text(self, line):
        for part in line.splitlines():
            frame = json.loads(part)
            self.frames.append(frame)
            if "id" in frame:
                self.replies[frame["id"]] = frame
                self.reply_order.append(frame["id"])
                self.reply_event(frame["id"]).set()

    async def receive_text(self):
        request = await self.inbound.get()
        if request is None:
            raise ws_mod._WebSocketDisconnect()
        return json.dumps(request)

    async def close(self):
        self.closed = True


class Raw(str):
    """An inbound item sent verbatim (not json.dumps'd): JSON null, a list, a bare string."""


async def _receive_raw(self):
    request = await self.inbound.get()
    if request is None:
        raise ws_mod._WebSocketDisconnect()
    return str(request) if isinstance(request, Raw) else json.dumps(request)


class _FailingReplies(MemorySocket):
    """Replies with the given ids cannot be written (the peer is gone for them)."""

    def __init__(self, authority, fail_ids):
        super().__init__(authority)
        self.fail_ids = set(fail_ids)

    async def send_text(self, line):
        for part in line.splitlines():
            if json.loads(part).get("id") in self.fail_ids:
                raise RuntimeError("peer gone")
        await super().send_text(line)


class _DeadAfterHangUp(MemorySocket):
    """A real socket after the peer left: every send raises."""

    def __init__(self, authority):
        super().__init__(authority)
        self.gone = False

    async def receive_text(self):
        request = await self.inbound.get()
        if request is None:
            self.gone = True
            raise ws_mod._WebSocketDisconnect()
        return json.dumps(request)

    async def send_text(self, line):
        if self.gone:
            raise RuntimeError("socket closed")
        await super().send_text(line)


class _StartingRegistry:
    """One profile still starting (hot-serve), no running siblings."""

    def __init__(self, key, started):
        self.key, self.started = key, started

    def __iter__(self):
        return iter(())

    def starting(self, profile):
        from hermes_cli.profiles import profile_matches_home
        if profile_matches_home(profile, Path(self.key)) and not self.started.is_set():
            return self.key, self.started
        return None


def _noop(*args, **kwargs):
    pass


def _not_held(actor, ref, capability):
    raise AuthorityRefusal("not_found")


async def _never_admitted(actor, submission, **kwargs):
    # Looked up before ``AuthorityConnection.submission`` builds the argument, which refuses a socket
    # that never attached the chat: the owner's admission is never reached here.
    raise AssertionError("prompt.submit reached the owner's admission")


@pytest.fixture
def rig(monkeypatch):
    authority = SimpleNamespace(
        profile_id="repro-profile", instance_id="repro-instance", events={},
        sessions={"repro-session": object()},
        # session.resume of a session this owner does not hold: no stored row, then the read gate refuses.
        persistence=SimpleNamespace(sessions=SimpleNamespace(get=lambda session_id: None)),
        authorize=_not_held,
        submit=_never_admitted,
    )
    gates, entered, log = {}, [], []

    async def gated_mutation(bound_authority, actor, ref, params):
        assert bound_authority is authority
        assert actor.capabilities and "session:control" in actor.capabilities
        entered.append(params["request_id"])
        log.append(f"start:{params['request_id']}")
        gate = gates.get(params["request_id"])
        if gate is not None:
            await gate.wait()
        log.append(f"done:{params['request_id']}")
        return {"session_id": ref.session_id, "operation": params["operation"], "revision": 1}

    monkeypatch.setattr(session_mutations, "mutate_session", gated_mutation)
    # Housekeeping is unrelated to dispatch and must not start background services.
    monkeypatch.setattr(ws_mod, "_note_dashboard_client_activity", _noop)
    monkeypatch.setattr(ws_mod, "_start_backend_liveness", _noop)
    monkeypatch.setattr(server, "resolve_skin", dict)
    monkeypatch.setattr(server, "_skin_home_for", lambda profile: None)
    for name in ("_ensure_skin_watcher", "_ensure_lease_watcher",
                 "register_live_transport", "unregister_live_transport", "_release_wake_for_transport"):
        monkeypatch.setattr(server, name, _noop)
    monkeypatch.setattr(server, "_close_sessions_for_transport",
                        lambda *args, **kwargs: (log.append("teardown"), (0, 0))[1])
    return SimpleNamespace(authority=authority, gates=gates, entered=entered, log=log,
                           socket=MemorySocket(authority))


def _start(rig, **identity):
    authority = rig.authority
    return asyncio.create_task(ws_mod.handle_ws(rig.socket, auth_identity={
        "user_id": "repro-user", "provider": "local", "profile_id": authority.profile_id,
        "instance_id": authority.instance_id, **identity,
    }), name="repro-websocket")


def _mutate(req_id, request_id, operation="compress", session_id="repro-session", payload=None):
    return {"jsonrpc": "2.0", "id": req_id, "method": "session.mutate",
            "params": {"session_id": session_id, "request_id": request_id,
                       "expected_revision": 0, "expected_generation": 0,
                       "operation": operation, "payload": payload if payload is not None else {}}}


def _req(req_id, method, params):
    return {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}


async def _settle(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            return False
        await asyncio.sleep(0.01)
    return True


async def _let_done_callbacks_run(turns=10):
    """Yield to the loop so a finished chained task's done-callback has run."""
    for _ in range(turns):
        await asyncio.sleep(0)


async def _finish(rig, task, *gates):
    for gate in gates:
        gate.set()
    rig.socket.inbound.put_nowait(None)
    try:
        await asyncio.wait_for(task, 5)
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def _order(rig, ids):
    return [rid for rid in rig.socket.reply_order if rid in ids]


# ── the lane stays free ──────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "params", "expected"),
    [
        ("gateway.ping", {}, {"result": {"ok": True}}),
        ("ping", {}, {"result": {"pong": True}}),
        ("session.control.read", {"session_id": "other-session"}, {"reason": "not_found"}),
        ("session.resume", {"session_id": "other-session"}, {"reason": "not_found"}),
        ("prompt.submit", {"session_id": "other-session", "text": "hi", "submission_id": "sub-1"}, {"reason": "permission_denied"}),
    ],
    ids=["gateway-heartbeat", "desktop-legacy-liveness", "control-read-other-session", "resume-other-session",
         "submit-other-session"],
)
async def test_rpc_answers_while_compression_is_pending(rig, method, params, expected):
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    responded_while_pending = False
    try:
        rig.socket.inbound.put_nowait(_mutate("mutation", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait({"jsonrpc": "2.0", "id": "probe", "method": method, "params": params})
        try:
            await asyncio.wait_for(rig.socket.reply_event("probe").wait(), 5)
            responded_while_pending = not release.is_set()
        except TimeoutError:
            pass  # released and drained below before the assertion reports it
    finally:
        await _finish(rig, task, release)

    assert rig.socket.closed and rig.authority.events == {}
    assert rig.socket.replies["mutation"]["result"]["operation"] == "compress"
    probe = rig.socket.replies["probe"]
    if "result" in expected:
        assert probe.get("result") == expected["result"], probe
    else:
        assert (probe.get("error") or {}).get("data", {}).get("reason") == expected["reason"], probe
    assert responded_while_pending, (
        f"{method} waited for the pending compression on the serial lane; it answered only after "
        f"release: {probe}")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "payload"),
    [("rename", {"title": "renamed"}), ("model", {"model": "other-model"}), ("archive", {"archived": True})],
    ids=["rename-other-session", "model-pick-other-session", "archive-other-session"],
)
async def test_a_mutation_of_another_session_answers_while_compression_is_pending(rig, operation, payload):
    """Renaming, re-modeling or archiving another chat neither waits for the compression nor joins it."""
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait(_mutate("other", "other-1", operation, "other-session", payload))
        assert await _settle(lambda: "other" in rig.socket.replies), rig.log
        assert not release.is_set() and "first" not in rig.socket.replies
        # It ran on the serial lane: a ping read after it answers after it, still under the compression.
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        assert await _settle(lambda: "probe" in rig.socket.replies)
    finally:
        await _finish(rig, task, release)
    assert rig.log[:3] == ["start:compress-1", "start:other-1", "done:other-1"], rig.log
    assert _order(rig, {"first", "other", "probe"}) == ["other", "probe", "first"]
    assert rig.socket.replies["other"]["result"]["operation"] == operation


@pytest.mark.asyncio
async def test_a_mutation_of_another_session_keeps_the_serial_lane_order(rig):
    """Off the chain means on the serial lane, not beside it: a slow edit of another chat still holds
    the frames read after it, as before the change."""
    rig.gates["compress-1"] = release = asyncio.Event()
    rig.gates["other-1"] = other_gate = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait(_mutate("other", "other-1", "rename", "other-session", {"title": "x"}))
        assert await _settle(lambda: "other-1" in rig.entered)
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        assert not await _settle(lambda: "probe" in rig.socket.replies, timeout=0.5), \
            "a ping overtook the edit on the serial lane"
        other_gate.set()
        assert await _settle(lambda: "probe" in rig.socket.replies)
        assert not release.is_set()
    finally:
        await _finish(rig, task, release, other_gate)


@pytest.mark.asyncio
async def test_the_lane_stays_free_behind_a_queued_mutation(rig):
    """A mutation queued behind the compression waits off the lane too: a read after both answers."""
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait(_mutate("second", "rename-1", "rename", payload={"title": "x"}))
        rig.socket.inbound.put_nowait({"jsonrpc": "2.0", "id": "probe", "method": "ping", "params": {}})
        assert await _settle(lambda: "probe" in rig.socket.replies, timeout=5.0), rig.log
        assert not release.is_set() and "rename-1" not in rig.entered
    finally:
        await _finish(rig, task, release)
    assert rig.log[:4] == ["start:compress-1", "done:compress-1", "start:rename-1", "done:rename-1"]


@pytest.mark.asyncio
async def test_odd_frames_during_a_pending_compression(rig, monkeypatch):
    """Malformed frames and mutations without a usable session id stay on the serial lane (the contract
    refuses them at once); only the well-formed edit of the compressing chat waits."""
    monkeypatch.setattr(MemorySocket, "receive_text", _receive_raw)
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        for raw in ("null", "[1, 2]", '"text"', "42"):
            rig.socket.inbound.put_nowait(Raw(raw))
        rig.socket.inbound.put_nowait({"jsonrpc": "2.0", "id": "nomethod"})
        rig.socket.inbound.put_nowait(_req("badparams", "session.mutate", None))
        rig.socket.inbound.put_nowait(_req("listparams", "prompt.submit", [1]))
        rig.socket.inbound.put_nowait(_req("intsid", "session.mutate", {"session_id": 7, "operation": "rename"}))
        rig.socket.inbound.put_nowait(_req("nosid", "session.mutate", {"operation": "compress"}))
        rig.socket.inbound.put_nowait(_mutate("rename", "rename-1", "rename", payload={"title": "x"}))
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        answered = {"probe", "nomethod", "badparams", "listparams", "intsid", "nosid"}
        assert await _settle(lambda: answered <= set(rig.socket.replies)), rig.socket.reply_order
        assert "rename-1" not in rig.entered and not release.is_set()
    finally:
        await _finish(rig, task, release)
    assert rig.log[:4] == ["start:compress-1", "done:compress-1", "start:rename-1", "done:rename-1"], rig.log


@pytest.mark.asyncio
async def test_a_mutation_without_a_usable_session_id_keeps_the_serial_lane_order(rig, monkeypatch):
    """Such a frame never reaches a session (the contract refuses it), so it has nothing to follow: it stays
    on the serial lane, and a frame read after it still waits for it, as before the change."""
    odd_gate = asyncio.Event()

    async def gated_dispatch(authority_connection, req, req_method, transport):
        if req.get("id") == "odd":
            await odd_gate.wait()
        return await ORIGINAL_DISPATCH(authority_connection, req, req_method, transport)

    monkeypatch.setattr(ws_mod, "_dispatch_request", gated_dispatch)
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait(_req("odd", "session.mutate", {"session_id": 7, "operation": "compress"}))
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        assert not await _settle(lambda: "probe" in rig.socket.replies, timeout=0.5), \
            "a ping overtook the malformed mutation: it left the serial lane"
        odd_gate.set()
        assert await _settle(lambda: {"odd", "probe"} <= set(rig.socket.replies))
        assert not release.is_set() and "first" not in rig.socket.replies
    finally:
        await _finish(rig, task, release, odd_gate)
    assert "error" in rig.socket.replies["odd"]


# ── same-session ordering ────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("second", "payload"),
    [("rename", {"title": "renamed"}), ("model", {"model": "other-model"}), ("compress", {})],
    ids=["rename-same-session", "model-pick-same-session", "second-compress-same-session"],
)
async def test_a_later_mutation_of_the_compressing_session_waits_for_it(rig, second, payload):
    """Mutations of one session keep arrival order: the second never starts before the compression
    ended (its commit would race the compression's revision fence), and its reply follows."""
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait(_mutate("second", "second-1", second, "repro-session", payload))
        # Give the second every chance to overtake: it must not.
        assert not await _settle(lambda: "second-1" in rig.entered, timeout=1.0), rig.log
        assert "second" not in rig.socket.replies
    finally:
        await _finish(rig, task, release)

    assert rig.log[:4] == ["start:compress-1", "done:compress-1", "start:second-1", "done:second-1"], rig.log
    assert _order(rig, {"first", "second"}) == ["first", "second"]
    assert rig.socket.replies["second"]["result"]["operation"] == second


@pytest.mark.asyncio
async def test_frames_for_the_compressing_session_keep_their_order_except_reads(rig):
    """A send to the chat being compressed still follows the compression (as on the serial lane); its
    control read answers beside it."""
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait({"jsonrpc": "2.0", "id": "submit", "method": "prompt.submit", "params": {
            "session_id": "repro-session", "text": "hi", "submission_id": "sub-1"}})
        rig.socket.inbound.put_nowait({"jsonrpc": "2.0", "id": "read", "method": "session.control.read",
                                       "params": {"session_id": "repro-session"}})
        assert await _settle(lambda: "read" in rig.socket.replies, timeout=5.0), rig.socket.reply_order
        assert not release.is_set() and "submit" not in rig.socket.replies
    finally:
        await _finish(rig, task, release)
    assert _order(rig, {"first", "submit"}) == ["first", "submit"], rig.socket.reply_order


@pytest.mark.asyncio
async def test_interrupt_and_resume_of_the_compressing_session_follow_it(rig):
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait(_req("interrupt", "session.interrupt", {"session_id": "repro-session"}))
        rig.socket.inbound.put_nowait(_req("resume", "session.resume", {"session_id": "repro-session"}))
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        assert await _settle(lambda: "probe" in rig.socket.replies)
        assert "interrupt" not in rig.socket.replies and "resume" not in rig.socket.replies
    finally:
        await _finish(rig, task, release)
    assert _order(rig, {"first", "interrupt", "resume"}) == ["first", "interrupt", "resume"], rig.socket.reply_order


@pytest.mark.asyncio
async def test_a_compression_of_another_session_runs_on_its_own_chain(rig):
    """Two chats compressed back to back: the second leaves the lane at once and does not wait for the
    first; a later edit of the second chat follows only its own compression."""
    rig.gates["compress-a"] = release_a = asyncio.Event()
    rig.gates["compress-b"] = release_b = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("a", "compress-a", session_id="repro-session"))
        assert await _settle(lambda: "compress-a" in rig.entered)
        rig.socket.inbound.put_nowait(_mutate("b", "compress-b", session_id="other-session"))
        assert await _settle(lambda: "compress-b" in rig.entered), "the second compression waited for the first"
        rig.socket.inbound.put_nowait(_mutate("rename-b", "rename-b", "rename", "other-session", {"title": "x"}))
        rig.socket.inbound.put_nowait(_req("read-b", "session.control.read", {"session_id": "other-session"}))
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        assert await _settle(lambda: {"probe", "read-b"} <= set(rig.socket.replies)), rig.socket.reply_order
        assert "rename-b" not in rig.entered, "an edit overtook its own chat's compression"
        release_b.set()
        assert await _settle(lambda: "rename-b" in rig.socket.replies), rig.log
        assert not release_a.is_set() and "a" not in rig.socket.replies
    finally:
        await _finish(rig, task, release_a, release_b)
    assert _order(rig, {"a", "b", "rename-b"}) == ["b", "rename-b", "a"]
    assert rig.log.index("done:compress-b") < rig.log.index("start:rename-b") < rig.log.index("done:compress-a")


@pytest.mark.asyncio
async def test_a_failing_compression_does_not_break_the_chain(rig, monkeypatch):
    release = asyncio.Event()

    async def failing(bound_authority, actor, ref, params):
        rig.entered.append(params["request_id"])
        rig.log.append(f"start:{params['request_id']}")
        if params["operation"] == "compress":
            await release.wait()
            raise RuntimeError("summary provider exploded")
        rig.log.append(f"done:{params['request_id']}")
        return {"session_id": ref.session_id, "operation": params["operation"], "revision": 1}

    monkeypatch.setattr(session_mutations, "mutate_session", failing)
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait(_mutate("second", "rename-1", "rename", payload={"title": "x"}))
        release.set()
        assert await _settle(lambda: "second" in rig.socket.replies), rig.socket.reply_order
        # Chain drained: a frame for the session afterwards runs on the serial lane and answers.
        rig.socket.inbound.put_nowait(_req("after", "session.control.read", {"session_id": "repro-session"}))
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        assert await _settle(lambda: {"after", "probe"} <= set(rig.socket.replies))
    finally:
        await _finish(rig, task, release)
    assert rig.socket.replies["first"]["error"]["code"] == -32603
    assert rig.socket.replies["second"]["result"]["operation"] == "rename"


# ── the chain ends ───────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_mutations_stay_on_the_serial_lane_without_a_pending_compression(rig):
    """Only a compression starts a chain, and none is left once it ends, so a later mutation keeps the
    serial lane's ordering (a ping read after it waits for it)."""
    rig.gates["rename-0"] = rename_0 = asyncio.Event()
    rig.gates["rename-1"] = rename_1 = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("r0", "rename-0", "rename", payload={"title": "a"}))
        assert await _settle(lambda: "rename-0" in rig.entered)
        rig.socket.inbound.put_nowait(_req("p0", "ping", {}))
        assert not await _settle(lambda: "p0" in rig.socket.replies, timeout=0.5), "a rename left the lane"
        rename_0.set()
        assert await _settle(lambda: "p0" in rig.socket.replies)
        # A compression that already finished leaves nothing behind.
        rig.socket.inbound.put_nowait(_mutate("c", "compress-1"))
        assert await _settle(lambda: "c" in rig.socket.replies)
        await _let_done_callbacks_run()
        rig.socket.inbound.put_nowait(_mutate("r1", "rename-1", "rename", payload={"title": "b"}))
        assert await _settle(lambda: "rename-1" in rig.entered)
        rig.socket.inbound.put_nowait(_req("p1", "ping", {}))
        assert not await _settle(lambda: "p1" in rig.socket.replies, timeout=0.5), \
            "a rename after a finished compression left the lane"
    finally:
        await _finish(rig, task, rename_0, rename_1)


@pytest.mark.asyncio
async def test_a_session_leaves_the_chain_when_its_compression_ends(rig, monkeypatch):
    """Once the compression finished, a frame naming that session is back on the serial lane (a ping
    read after a slow ``session.info`` of it waits, as before the change)."""
    info_gate = asyncio.Event()

    async def gated_dispatch(authority_connection, req, req_method, transport):
        if req_method == "session.info":
            await info_gate.wait()
            return {"jsonrpc": "2.0", "id": req.get("id"), "result": {}}
        return await ORIGINAL_DISPATCH(authority_connection, req, req_method, transport)

    monkeypatch.setattr(ws_mod, "_dispatch_request", gated_dispatch)
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("c", "compress-1"))
        assert await _settle(lambda: "c" in rig.socket.replies)
        await _let_done_callbacks_run()
        rig.socket.inbound.put_nowait(_req("info", "session.info", {"session_id": "repro-session"}))
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        assert not await _settle(lambda: "probe" in rig.socket.replies, timeout=0.5), \
            "the session stayed chained after its compression ended"
    finally:
        await _finish(rig, task, info_gate)


@pytest.mark.asyncio
@pytest.mark.parametrize("parks", [False, True], ids=["at-the-limit", "one-past-the-limit"])
async def test_the_chain_limit_counts_the_running_compression(rig, parks):
    """A documented bound, not a goal: ``_WS_MUTATION_CHAIN_LIMIT`` chained frames, the running
    compression included, fit off the lane; the next one parks the serial lane until a chained frame
    ends (the old serial behavior, so a flood cannot grow the chain without bound)."""
    limit = ws_mod._WS_MUTATION_CHAIN_LIMIT
    queued = limit if parks else limit - 1  # frames behind the compression
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        for n in range(queued):
            rig.socket.inbound.put_nowait(_mutate(f"r{n}", f"rename-{n}", "rename", payload={"title": str(n)}))
        rig.socket.inbound.put_nowait(_req("probe", "ping", {}))
        answered = await _settle(lambda: "probe" in rig.socket.replies, timeout=1.0)
        assert answered is not parks, f"queued={queued} limit={limit}"
        release.set()
        assert await _settle(lambda: "probe" in rig.socket.replies)
    finally:
        await _finish(rig, task, release)
    starts = [entry for entry in rig.log if entry.startswith("start:")]
    assert starts == ["start:compress-1"] + [f"start:rename-{n}" for n in range(queued)]


# ── hang-up and shutdown ─────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_hang_up_finishes_the_pending_compression_before_teardown(rig):
    """As the serial lane drained its in-flight handler: the reply is attempted and the authority
    connection closes only afterwards."""
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
    assert await _settle(lambda: "compress-1" in rig.entered)
    rig.socket.inbound.put_nowait(None)
    assert not await _settle(lambda: "teardown" in rig.log, timeout=1.0), "teardown ran under a pending compression"
    assert rig.authority.events, "the authority connection closed under a pending compression"
    release.set()
    await asyncio.wait_for(task, 5)
    assert rig.log.index("done:compress-1") < rig.log.index("teardown")
    assert rig.socket.replies["first"]["result"]["operation"] == "compress"
    assert rig.authority.events == {}


@pytest.mark.asyncio
async def test_hang_up_with_a_flood_past_the_limit_finishes_everything_in_order(rig):
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    flood = ws_mod._WS_MUTATION_CHAIN_LIMIT + 8
    rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
    assert await _settle(lambda: "compress-1" in rig.entered)
    for n in range(flood):
        rig.socket.inbound.put_nowait(_mutate(f"r{n}", f"rename-{n}", "rename", payload={"title": str(n)}))
    rig.socket.inbound.put_nowait(None)
    assert not await _settle(lambda: "teardown" in rig.log, timeout=0.5)
    release.set()
    await asyncio.wait_for(task, 10)
    starts = [entry for entry in rig.log if entry.startswith("start:")]
    assert starts == ["start:compress-1"] + [f"start:rename-{n}" for n in range(flood)]
    assert rig.log.index(f"done:rename-{flood - 1}") < rig.log.index("teardown")
    assert all(f"r{n}" in rig.socket.replies for n in range(flood))
    assert rig.authority.events == {}


@pytest.mark.asyncio
async def test_a_failed_chained_reply_ends_the_connection(rig):
    """The compression's reply cannot be written, so the reader stops (as the concurrent read lane
    does) instead of waiting on a socket nobody drains."""
    rig.socket = _FailingReplies(rig.authority, {"first"})
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        release.set()
        done, _ = await asyncio.wait({task}, timeout=5)
        assert task in done, "the connection kept reading after a chained reply failed"
        assert "teardown" in rig.log
    finally:
        await _finish(rig, task, release)


@pytest.mark.asyncio
async def test_hang_up_does_not_dispatch_queued_frames_once_a_reply_failed(rig):
    """On the serial lane, the compression's failed reply ends the dispatcher, so frames queued behind
    it are never run for a client that left. The chain skips them too."""
    rig.socket = _DeadAfterHangUp(rig.authority)
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
    assert await _settle(lambda: "compress-1" in rig.entered)
    rig.socket.inbound.put_nowait(_mutate("second", "rename-1", "rename", payload={"title": "x"}))
    rig.socket.inbound.put_nowait(_mutate("third", "rename-2", "rename", payload={"title": "y"}))
    await asyncio.sleep(0.2)  # health: allow HX013 -- the reader must have read both queued frames before the hang-up; nothing observable marks that
    rig.socket.inbound.put_nowait(None)
    await asyncio.sleep(0.2)  # health: allow HX013 -- the hang-up must have been read (and the peer marked gone) before the compression ends
    release.set()
    await asyncio.wait_for(task, 5)
    assert "rename-1" not in rig.entered and "rename-2" not in rig.entered, \
        f"a queued mutation ran after the client left: {rig.log}"
    assert "teardown" in rig.log


@pytest.mark.asyncio
async def test_server_shutdown_does_not_wait_for_a_pending_compression(rig):
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=5)
        assert task in done, "socket shutdown waited for the pending compression"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_shutdown_cancels_the_chain_but_the_shielded_mutation_finishes(rig, monkeypatch):
    """The real ``mutate_session`` (track_mutation + shield) around a gated owned mutation."""
    release = asyncio.Event()
    rig.authority._require_admission_open = lambda **kwargs: None

    async def owned(bound_authority, actor, ref, params):
        rig.entered.append(params["request_id"])
        rig.log.append(f"start:{params['request_id']}")
        if params["operation"] == "compress":
            await release.wait()
        rig.log.append(f"done:{params['request_id']}")
        return {"session_id": ref.session_id, "operation": params["operation"], "revision": 1}

    monkeypatch.setattr(session_mutations, "mutate_session", ORIGINAL_MUTATE)
    monkeypatch.setattr(session_mutations, "_owned_mutation", owned)
    task = _start(rig)
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        rig.socket.inbound.put_nowait(_mutate("second", "rename-1", "rename", payload={"title": "x"}))
        await asyncio.sleep(0.2)  # health: allow HX013 -- the queued rename must be parked behind the compression before shutdown
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=5)
        assert task in done, "shutdown waited for the chain"
        release.set()
        assert await _settle(lambda: "done:compress-1" in rig.log), "the shielded mutation was cancelled"
        await asyncio.sleep(0.1)  # health: allow HX013 -- proving a negative: the cancelled queued frame must stay unrun
        assert "rename-1" not in rig.entered, "a queued chained frame ran after shutdown"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_frame_for_a_starting_profile_waits_for_the_start_not_the_chain(rig):
    """The profile-start hold comes first, so a mutation naming a profile that is still starting waits
    for that start in its own lane even while a compression holds the chain."""
    from hermes_cli.profiles import get_profile_dir
    started = asyncio.Event()
    key = str(get_profile_dir("startingprof"))
    rig.authority.runner = SimpleNamespace(session_authorities=_StartingRegistry(key, started))
    rig.gates["compress-1"] = release = asyncio.Event()
    task = _start(rig, profile_scope="host")
    try:
        rig.socket.inbound.put_nowait(_mutate("first", "compress-1"))
        assert await _settle(lambda: "compress-1" in rig.entered)
        held = _mutate("held", "rename-held", "rename", payload={"title": "x"})
        held["params"]["profile"] = "startingprof"
        rig.socket.inbound.put_nowait(held)
        await asyncio.sleep(0.1)  # health: allow HX013 -- the held frame must be read and parked in its profile-start lane before the compression ends
        release.set()
        assert await _settle(lambda: "first" in rig.socket.replies)
        assert not await _settle(lambda: "held" in rig.socket.replies, timeout=0.5), \
            f"the frame for a starting profile ran before its start ended: {rig.socket.replies.get('held')}"
        started.set()
        assert await _settle(lambda: "held" in rig.socket.replies)
    finally:
        await _finish(rig, task, release, started)
