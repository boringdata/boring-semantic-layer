import ibis
import pytest
from ibis import _

from boring_semantic_layer import to_semantic_table


def _opportunities_model():
    source = ibis.memtable(
        {
            "status": ["open", "closed", "open"],
            "private_amount": [100, 200, 300],
        }
    )
    return (
        to_semantic_table(source, name="opportunities")
        .with_dimensions(status=_.status)
        .with_measures(
            opportunity_count=_.count(),
            pipeline_amount=_.private_amount.sum(),
        )
    )


def test_strict_semantic_boundaries_reject_raw_dimension():
    opportunities = _opportunities_model()

    with pytest.raises(ValueError, match="private_amount"):
        opportunities.query(
            dimensions=["private_amount"],
            measures=["opportunity_count"],
            strict_semantic_boundaries=True,
        )


def test_strict_semantic_boundaries_reject_raw_filter_field():
    opportunities = _opportunities_model()

    with pytest.raises(ValueError, match="private_amount"):
        opportunities.query(
            dimensions=["status"],
            measures=["opportunity_count"],
            filters=[{"field": "private_amount", "operator": ">=", "value": 200}],
            strict_semantic_boundaries=True,
        )


def test_strict_semantic_boundaries_allow_model_prefixed_declared_filter():
    opportunities = _opportunities_model()

    result = opportunities.query(
        dimensions=["opportunities.status"],
        measures=["opportunity_count"],
        filters=[{"field": "opportunities.status", "operator": "=", "value": "open"}],
        strict_semantic_boundaries=True,
    ).execute()

    assert list(result["status"]) == ["open"]


def test_index_uses_plain_table_expressions_for_weight_and_value_refs():
    opportunities = _opportunities_model()

    result = opportunities.index("status").execute()

    assert set(result["fieldValue"]) == {"open", "closed"}
    assert result.set_index("fieldValue").loc["open", "weight"] == 2


def test_profile_constructor_value_error_falls_back_to_plain_ibis(monkeypatch):
    import ibis as ibis_module

    import boring_semantic_layer.profile as profile_module

    class RaisingXorqProfile:
        def __init__(self, *args, **kwargs):
            raise ValueError("unsupported by xorq")

    class FakeBackend:
        @staticmethod
        def connect(**kwargs):
            return {"connected_with": kwargs}

    monkeypatch.setattr(profile_module, "XorqProfile", RaisingXorqProfile)
    monkeypatch.setattr(ibis_module, "fakebackend", FakeBackend, raising=False)

    connection = profile_module._create_connection_from_config(
        {"type": "fakebackend", "token": "${FAKE_TOKEN}"}
    )

    assert connection == {"connected_with": {"token": "${FAKE_TOKEN}"}}


def test_profile_connection_value_error_is_not_swallowed(monkeypatch):
    import boring_semantic_layer.profile as profile_module

    class RaisingXorqProfile:
        def __init__(self, *args, **kwargs):
            pass

        def get_con(self):
            raise ValueError("invalid connection option")

    monkeypatch.setattr(profile_module, "XorqProfile", RaisingXorqProfile)

    with pytest.raises(ValueError, match="invalid connection option"):
        profile_module._create_connection_from_config({"type": "duckdb"})
