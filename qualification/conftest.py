"""Fail closed when the dedicated qualification lacks exact accepted checkouts."""
import importlib
import os
from pathlib import Path
import subprocess
import pytest

@pytest.fixture(scope="session", autouse=True)
def exact_merged_dependencies():
    assert os.environ.get("GAX_RUNTIME_COMPATIBILITY_PROFILE") == "merged-producers-v1"
    pins = {
        "agent_control_plane": "d3dadee70bd319812b207389ab1e0f6efe511916",
        "engine.producer_contract": "c3c3ee7188b9367cf70b08074b9c40a5c70c94ac",
        "agent_replay_bundle": "459e4ba62fca49364aebb0050cd5fb2dd5a71bfa",
        "odes": "c5e9a0f3695ae836b803be06c46d2c669642ee03",
        "agent_governance_evidence_pack": "b4baccd823d2a73be276c1de745b19cf7c56a0d6",
    }
    from experiments.odex_gax_imx_reference.gax_ref_runtime import load_executor_runtime
    load_executor_runtime()
    for name, expected in pins.items():
        module = importlib.import_module(name)
        actual = subprocess.check_output(["git", "-C", str(Path(module.__file__).parent), "rev-parse", "HEAD"], text=True, timeout=10).strip()
        assert actual == expected, (name, actual, expected)
    manifest = Path(os.environ["UPSTREAM_MANIFEST_EXAMPLE"])
    actual = subprocess.check_output(["git", "-C", str(manifest.parent), "rev-parse", "HEAD"], text=True, timeout=10).strip()
    assert actual == "46c950bed37fe3812000895430bc0312d29e37ce"
