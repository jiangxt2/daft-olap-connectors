# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import pytest

from scripts import wait_for_service

_BACKEND_DESCRIPTION = ("Alive", "AvailCapacity", "TotalCapacity")


def test_doris_backend_requires_liveness_and_reported_storage() -> None:
    assert wait_for_service._doris_backends_ready(
        _BACKEND_DESCRIPTION,
        [("true", "781.806 GB", "910.737 GB")],
    )
    assert wait_for_service._doris_backends_ready(
        _BACKEND_DESCRIPTION,
        [("TRUE", "781.806 GB", "910.737 GB")],
    )
    assert not wait_for_service._doris_backends_ready(
        _BACKEND_DESCRIPTION,
        [("true", "0.000 ", "0.000 ")],
    )
    assert not wait_for_service._doris_backends_ready(
        _BACKEND_DESCRIPTION,
        [("false", "781.806 GB", "910.737 GB")],
    )
    assert not wait_for_service._doris_backends_ready(
        _BACKEND_DESCRIPTION,
        [
            ("true", "781.806 GB", "910.737 GB"),
            ("true", "0.000 ", "0.000 "),
        ],
    )


@pytest.mark.parametrize("capacity", ["", "unknown", "nan GB", "inf GB", "-1 GB"])
def test_doris_backend_rejects_invalid_capacity(capacity: str) -> None:
    assert not wait_for_service._doris_backends_ready(
        _BACKEND_DESCRIPTION,
        [("true", capacity, "910.737 GB")],
    )


def test_doris_backend_readiness_fails_closed_for_missing_metadata() -> None:
    assert not wait_for_service._doris_backends_ready(_BACKEND_DESCRIPTION, [])
    assert not wait_for_service._doris_backends_ready(
        ("Alive", "TotalCapacity"),
        [("true", "910.737 GB")],
    )
    assert not wait_for_service._doris_backends_ready(
        _BACKEND_DESCRIPTION,
        [("true",)],
    )
