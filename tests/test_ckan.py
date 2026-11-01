"""Tests for quantilica.core.ckan."""

from unittest.mock import MagicMock

import pytest

from quantilica.core.ckan import CkanClient, CkanError, pick_resources


def test_ckan_status_success():
    client_mock = MagicMock()
    client_mock.get_json.return_value = {
        "success": True,
        "result": {"version": "2.9.5", "site_title": "Portal de Dados Abertos"},
    }
    ckan = CkanClient("https://dados.gov.br", client=client_mock)
    status = ckan.status()
    assert status["version"] == "2.9.5"
    client_mock.get_json.assert_called_once_with(
        "https://dados.gov.br/api/3/action/status_show", params=None
    )


def test_ckan_api_error_raises_ckan_error():
    client_mock = MagicMock()
    client_mock.get_json.return_value = {
        "success": False,
        "error": {"message": "Not found"},
    }
    ckan = CkanClient("https://dados.gov.br", client=client_mock)
    with pytest.raises(CkanError, match="Not found"):
        ckan.package_show("unknown-dataset")


def test_ckan_iter_packages_paginated():
    client_mock = MagicMock()
    # First call returns 2 packages (count=3), second call returns 1 package
    client_mock.get_json.side_effect = [
        {
            "success": True,
            "result": {
                "count": 3,
                "results": [
                    {"id": "pkg-1", "name": "one"},
                    {"id": "pkg-2", "name": "two"},
                ],
            },
        },
        {
            "success": True,
            "result": {
                "count": 3,
                "results": [{"id": "pkg-3", "name": "three"}],
            },
        },
    ]

    ckan = CkanClient("https://dados.gov.br", client=client_mock)
    packages = list(ckan.iter_packages(rows=2))
    assert len(packages) == 3
    assert [p["name"] for p in packages] == ["one", "two", "three"]
    assert client_mock.get_json.call_count == 2


def test_pick_resources_preference_and_dedupe():
    package = {
        "resources": [
            {"id": "r1", "name": "vendas_2024", "format": "CSV", "size": 1000},
            {"id": "r2", "name": "vendas_2024", "format": "PARQUET", "size": 200},
            {"id": "r3", "name": "vendas_2024", "format": "ZIP", "size": 300},
            {"id": "r4", "name": "compras_2024", "format": "CSV", "size": 500},
            # duplicate, larger:
            {"id": "r5", "name": "compras_2024", "format": "CSV", "size": 1500},
        ]
    }

    picked = pick_resources(package, prefer=("PARQUET", "CSV", "ZIP"))
    assert len(picked) == 2

    # For vendas_2024, PARQUET should be chosen
    vendas = next(r for r in picked if r["name"] == "vendas_2024")
    assert vendas["id"] == "r2"
    assert vendas["format"] == "PARQUET"

    # For compras_2024, both are CSV, larger (1500) should be chosen
    compras = next(r for r in picked if r["name"] == "compras_2024")
    assert compras["id"] == "r5"
    assert compras["size"] == 1500
