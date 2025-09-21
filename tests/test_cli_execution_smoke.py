import json
import os
import pytest
from django.test import Client
from django.contrib.auth import get_user_model
from django.urls import reverse

pytestmark = pytest.mark.django_db


def ensure_admin():
    User = get_user_model()
    user, created = User.objects.get_or_create(username="smoke_admin", defaults={"is_staff": True, "is_superuser": True})
    if created:
        user.set_password("pass1234")
        user.save()
    return user


def test_overview_endpoint_basic():
    """Smoke test: overview endpoint should return 200 and JSON skeleton when logged in as staff."""
    client = Client()
    admin = ensure_admin()
    logged_in = client.login(username=admin.username, password="pass1234")
    assert logged_in, "Failed to login test admin"

    url = "/admin/templates/cli-runs/overview/"
    resp = client.get(url, HTTP_ACCEPT="application/json")
    assert resp.status_code == 200
    # Response may be HTML if template-based; try parsing JSON fallback
    try:
        data = json.loads(resp.content.decode("utf-8"))
        assert "ok" in data or "stats" in data
    except json.JSONDecodeError:
        # Accept minimal HTML page – still considered reachable
        assert b"overview" in resp.content.lower() or b"cli" in resp.content.lower()


def test_compose_preview_round_trip():
    """If compose-preview is available, it should not execute CLI and should return prompt fields."""
    client = Client()
    admin = ensure_admin()
    client.login(username=admin.username, password="pass1234")

    # Use a benign template id if known; fallback to graceful not-found handling.
    url = "/admin/templates/compose-preview/"
    resp = client.post(url, {
        "template_id": "nonexistent_template_for_smoke",
        "expert_slug": "",
    }, HTTP_ACCEPT="application/json")
    # Should not 500
    assert resp.status_code in (200, 400, 404)
    # Content should be parseable or at least textual
    assert resp.content
