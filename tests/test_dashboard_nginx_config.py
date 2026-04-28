from __future__ import annotations

from pathlib import Path


def test_dashboard_nginx_clears_upstream_connection_header() -> None:
    config_path = Path(__file__).resolve().parents[1] / "deploy" / "nginx" / "v5-web-dashboard.conf"
    config = config_path.read_text(encoding="utf-8")

    assert "proxy_http_version 1.0;" in config
    assert 'proxy_set_header Connection "";' in config
    assert "keepalive_timeout 0;" in config
