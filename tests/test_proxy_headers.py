"""proxy 头合并单测：网线上恰好一个鉴权头（渠道凭证），用户凭证绝不透出。

回归根因：客户端 `authorization`（小写）与注入的 `Authorization`（大写）
dict 并集大小写敏感 → 双 Authorization 头 → 上游边缘裸 400，且泄漏用户 sk。
"""

from starlette.requests import Request

from app.routers.proxy import _forward_headers
from app.services.upstream import auth_headers


def _request(headers: list[tuple[bytes, bytes]]) -> Request:
    return Request({"type": "http", "method": "POST", "headers": headers})


def _assert_single_auth(fwd: dict, expected_name: str, expected_value: str) -> None:
    auth_like = [(k, v) for k, v in fwd.items() if k.lower() == expected_name]
    assert auth_like == [(expected_name, expected_value)], f"鉴权头应恰好一个: {fwd}"


def test_bearer_user_token_never_forwarded(route_factory, key_lease_factory):
    """bearer：客户端小写 authorization 被剔除，网线上只有渠道 Authorization。"""
    route = route_factory(auth_type="bearer")
    key = key_lease_factory(key="mk-channel-key")
    req = _request([
        (b"authorization", b"Bearer sk-user-token"),
        (b"content-type", b"application/json"),
    ])
    fwd = _forward_headers(req, auth_headers(route, key))
    _assert_single_auth(fwd, "authorization", "Bearer mk-channel-key")
    assert "sk-user-token" not in fwd.values()


def test_x_api_key_case_collision(route_factory, key_lease_factory):
    """x-api-key：客户端 X-API-KEY 与注入的 X-Api-Key 大小写碰撞，extra 优先恰好一个。"""
    route = route_factory(auth_type="x-api-key")
    key = key_lease_factory(key="mk-channel-key")
    req = _request([
        (b"authorization", b"Bearer sk-user-token"),
        (b"x-api-key", b"user-supplied-key"),
    ])
    fwd = _forward_headers(req, auth_headers(route, key))
    _assert_single_auth(fwd, "x-api-key", "mk-channel-key")
    assert "user-supplied-key" not in fwd.values()
    assert "authorization" not in {k.lower() for k in fwd}


def test_header_override_wins_over_client(route_factory, key_lease_factory):
    """渠道 header_override 注入头同样按小写归一剔除客户端同义头。"""
    route = route_factory(auth_type="bearer")
    key = key_lease_factory(
        key="mk-channel-key",
        header_override={"X-Custom-Auth": "channel-value"},
    )
    req = _request([
        (b"x-custom-auth", b"client-value"),
        (b"x-keep-me", b"yes"),
    ])
    fwd = _forward_headers(req, auth_headers(route, key))
    assert fwd["x-custom-auth"] == "channel-value"
    assert sum(1 for k in fwd if k.lower() == "x-custom-auth") == 1
    assert fwd["x-keep-me"] == "yes"
