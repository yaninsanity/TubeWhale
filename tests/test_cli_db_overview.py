import pytest
from django.urls import reverse


@pytest.mark.django_db
def test_cli_db_overview_staff(client, django_user_model):
    user = django_user_model.objects.create_user(username="admin", password="x", is_staff=True, is_superuser=True)
    client.force_login(user)
    url = reverse('admin-cli-db-overview')
    resp = client.get(url)
    assert resp.status_code == 200
    # basic markers in template
    assert b'CLI External DB Overview' in resp.content
