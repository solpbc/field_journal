# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (c) 2026 sol pbc
"""VOiCES source selection from the Lab41/VOiCES-subset GitHub mirror.

This source pulls the eight VOiCES subset part tars from the Lab41 mirror.
The mirror uses a gzip-compressed tar payload despite the `.tar` extension,
so downloads must be opened with `tarfile.open(path, "r:gz")`.

We relaxed the original `rm1-none` filter to `rm1` with any distractor after
prep confirmed there are zero clean-distractor triplets in the full 8-tar
subset, but 46 same-utterance `rm1` triplets when distractors may differ
across mics. The locked fixture ships 15 of those triplets (45 WAVs).

Each shipped triplet is keyed by `(sp, sg)` and keeps `mc01`, `mc03`, and
`mc05`, but the upstream per-mic recordings can carry different distractors.
That distractor is documented per segment in the manifest description.

Day/time allocation uses five days and nine unique `07xxxx` slots per day to
avoid segment-key collisions. For triplet index `i`, day is `2026020{1+i//3}`;
within each day, triplet position `p = i % 3` uses `070000/070100/070200`,
`071000/071100/071200`, or `072000/072100/072200` for `mc01/mc03/mc05`.
"""

from __future__ import annotations

import os
import tarfile
from pathlib import Path
from typing import TypedDict

from tools.sources import download_file

BASE_URL = "https://raw.githubusercontent.com/Lab41/VOiCES-subset/master"
CSV_NAME = "Lab41-SRI-VOiCES_ref_SUBSET.csv"
MICS = ("mc01", "mc03", "mc05")
MIC_PRETTY = {"mc01": "close", "mc03": "mid", "mc05": "far"}
MIC_ORIENTATION = {"mc01": "clo", "mc03": "mid", "mc05": "far"}
DISTRACTOR_PRETTY = {
    "babb": "babble",
    "musi": "music",
    "none": "no",
    "tele": "telephone",
}


class MicInfo(TypedDict):
    distractor: str
    tar: str
    stem: str
    duration: int


class Triplet(TypedDict):
    sp: str
    sg: str
    mics: dict[str, MicInfo]


TRIPLETS: list[Triplet] = [
    {
        "sp": "sp0083",
        "sg": "sg0005",
        "mics": {
            "mc01": {
                "distractor": "musi",
                "tar": "VOiCES_90_3.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0083-ch003054-sg0005-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "babb",
                "tar": "VOiCES_90_5.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0083-ch003054-sg0005-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "none",
                "tar": "VOiCES_90_1.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0083-ch003054-sg0005-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
    {
        "sp": "sp0093",
        "sg": "sg0004",
        "mics": {
            "mc01": {
                "distractor": "tele",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0093-ch126208-sg0004-mc01-stu-clo-dg090",
                "duration": 16,
            },
            "mc03": {
                "distractor": "babb",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0093-ch126208-sg0004-mc03-stu-mid-dg090",
                "duration": 16,
            },
            "mc05": {
                "distractor": "babb",
                "tar": "VOiCES_90_1.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0093-ch126208-sg0004-mc05-stu-far-dg090",
                "duration": 16,
            },
        },
    },
    {
        "sp": "sp0150",
        "sg": "sg0026",
        "mics": {
            "mc01": {
                "distractor": "musi",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0150-ch126107-sg0026-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "none",
                "tar": "VOiCES_90_1.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0150-ch126107-sg0026-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "none",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0150-ch126107-sg0026-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
    {
        "sp": "sp0188",
        "sg": "sg0020",
        "mics": {
            "mc01": {
                "distractor": "tele",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0188-ch135249-sg0020-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "none",
                "tar": "VOiCES_90_5.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0188-ch135249-sg0020-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "babb",
                "tar": "VOiCES_90_1.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0188-ch135249-sg0020-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
    {
        "sp": "sp0196",
        "sg": "sg0018",
        "mics": {
            "mc01": {
                "distractor": "none",
                "tar": "VOiCES_90_2.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0196-ch122150-sg0018-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "musi",
                "tar": "VOiCES_90_5.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0196-ch122150-sg0018-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "tele",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0196-ch122150-sg0018-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
    {
        "sp": "sp0208",
        "sg": "sg0011",
        "mics": {
            "mc01": {
                "distractor": "musi",
                "tar": "VOiCES_90_4.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0208-ch126600-sg0011-mc01-stu-clo-dg090",
                "duration": 15,
            },
            "mc03": {
                "distractor": "babb",
                "tar": "VOiCES_90_6.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0208-ch126600-sg0011-mc03-stu-mid-dg090",
                "duration": 15,
            },
            "mc05": {
                "distractor": "babb",
                "tar": "VOiCES_90_4.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0208-ch126600-sg0011-mc05-stu-far-dg090",
                "duration": 15,
            },
        },
    },
    {
        "sp": "sp0242",
        "sg": "sg0004",
        "mics": {
            "mc01": {
                "distractor": "babb",
                "tar": "VOiCES_90_1.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0242-ch122625-sg0004-mc01-stu-clo-dg090",
                "duration": 16,
            },
            "mc03": {
                "distractor": "musi",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0242-ch122625-sg0004-mc03-stu-mid-dg090",
                "duration": 16,
            },
            "mc05": {
                "distractor": "tele",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0242-ch122625-sg0004-mc05-stu-far-dg090",
                "duration": 16,
            },
        },
    },
    {
        "sp": "sp0250",
        "sg": "sg0020",
        "mics": {
            "mc01": {
                "distractor": "none",
                "tar": "VOiCES_90_2.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0250-ch142276-sg0020-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "musi",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0250-ch142276-sg0020-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "tele",
                "tar": "VOiCES_90_5.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0250-ch142276-sg0020-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
    {
        "sp": "sp0288",
        "sg": "sg0008",
        "mics": {
            "mc01": {
                "distractor": "none",
                "tar": "VOiCES_90_4.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0288-ch130994-sg0008-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "tele",
                "tar": "VOiCES_90_4.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0288-ch130994-sg0008-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "musi",
                "tar": "VOiCES_90_1.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0288-ch130994-sg0008-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
    {
        "sp": "sp0296",
        "sg": "sg0031",
        "mics": {
            "mc01": {
                "distractor": "babb",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0296-ch142727-sg0031-mc01-stu-clo-dg090",
                "duration": 16,
            },
            "mc03": {
                "distractor": "tele",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0296-ch142727-sg0031-mc03-stu-mid-dg090",
                "duration": 16,
            },
            "mc05": {
                "distractor": "musi",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0296-ch142727-sg0031-mc05-stu-far-dg090",
                "duration": 16,
            },
        },
    },
    {
        "sp": "sp0492",
        "sg": "sg0001",
        "mics": {
            "mc01": {
                "distractor": "none",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0492-ch131899-sg0001-mc01-stu-clo-dg090",
                "duration": 16,
            },
            "mc03": {
                "distractor": "tele",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0492-ch131899-sg0001-mc03-stu-mid-dg090",
                "duration": 16,
            },
            "mc05": {
                "distractor": "musi",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0492-ch131899-sg0001-mc05-stu-far-dg090",
                "duration": 16,
            },
        },
    },
    {
        "sp": "sp0652",
        "sg": "sg0011",
        "mics": {
            "mc01": {
                "distractor": "none",
                "tar": "VOiCES_90_5.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0652-ch130726-sg0011-mc01-stu-clo-dg090",
                "duration": 19,
            },
            "mc03": {
                "distractor": "tele",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0652-ch130726-sg0011-mc03-stu-mid-dg090",
                "duration": 19,
            },
            "mc05": {
                "distractor": "musi",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0652-ch130726-sg0011-mc05-stu-far-dg090",
                "duration": 19,
            },
        },
    },
    {
        "sp": "sp0882",
        "sg": "sg0033",
        "mics": {
            "mc01": {
                "distractor": "tele",
                "tar": "VOiCES_90_5.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0882-ch123268-sg0033-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "none",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0882-ch123268-sg0033-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "babb",
                "tar": "VOiCES_90_4.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp0882-ch123268-sg0033-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
    {
        "sp": "sp0948",
        "sg": "sg0009",
        "mics": {
            "mc01": {
                "distractor": "none",
                "tar": "VOiCES_90_4.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-none-sp0948-ch132705-sg0009-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "musi",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-musi-sp0948-ch132705-sg0009-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "tele",
                "tar": "VOiCES_90_5.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp0948-ch132705-sg0009-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
    {
        "sp": "sp1066",
        "sg": "sg0004",
        "mics": {
            "mc01": {
                "distractor": "tele",
                "tar": "VOiCES_90_8.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-tele-sp1066-ch005330-sg0004-mc01-stu-clo-dg090",
                "duration": 17,
            },
            "mc03": {
                "distractor": "babb",
                "tar": "VOiCES_90_7.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp1066-ch005330-sg0004-mc03-stu-mid-dg090",
                "duration": 17,
            },
            "mc05": {
                "distractor": "babb",
                "tar": "VOiCES_90_1.tar",
                "stem": "Lab41-SRI-VOiCES-rm1-babb-sp1066-ch005330-sg0004-mc05-stu-far-dg090",
                "duration": 17,
            },
        },
    },
]


def _needed_tar_names() -> list[str]:
    return sorted(
        {
            str(mic_info["tar"])
            for triplet in TRIPLETS
            for mic_info in triplet["mics"].values()
        }
    )


def _needed_stems() -> set[str]:
    return {
        str(mic_info["stem"])
        for triplet in TRIPLETS
        for mic_info in triplet["mics"].values()
    }


def _write_atomic(dest: Path, data: bytes) -> None:
    tmp_path = dest.with_suffix(dest.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    with open(tmp_path, "wb") as handle:
        handle.write(data)
    os.rename(tmp_path, dest)


def download(cache_dir: Path) -> None:
    """Download required VOiCES tar files and extract the locked WAV set."""
    source_dir = cache_dir / "voices"
    source_dir.mkdir(parents=True, exist_ok=True)

    download_file(f"{BASE_URL}/{CSV_NAME}", source_dir / CSV_NAME)

    needed_tar_names = _needed_tar_names()
    for tar_name in needed_tar_names:
        download_file(f"{BASE_URL}/{tar_name}", source_dir / tar_name)

    needed_stems = _needed_stems()
    if all((source_dir / f"{stem}.wav").exists() for stem in needed_stems):
        return

    remaining = {
        stem for stem in needed_stems if not (source_dir / f"{stem}.wav").exists()
    }
    for tar_name in needed_tar_names:
        if not remaining:
            break
        tar_path = source_dir / tar_name
        with tarfile.open(tar_path, "r:gz") as archive:
            for member in archive.getmembers():
                if not (member.isfile() and member.name.lower().endswith(".wav")):
                    continue
                basename = Path(member.name).name
                if basename.startswith("._"):
                    continue
                stem = basename[:-4]
                if stem not in remaining:
                    continue
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise RuntimeError(f"Unable to extract VOiCES member {member.name}")
                _write_atomic(source_dir / f"{stem}.wav", extracted.read())
                remaining.remove(stem)

    if remaining:
        raise RuntimeError(f"Missing VOiCES WAVs after extraction: {sorted(remaining)}")


def segments() -> list[dict]:
    """Return the locked VOiCES segment definitions."""
    segments_list: list[dict] = []
    for triplet_index, triplet in enumerate(TRIPLETS):
        day = f"2026020{(triplet_index // 3) + 1}"
        position = triplet_index % 3
        for mic_index, mic in enumerate(MICS):
            mic_info = triplet["mics"][mic]
            distractor = str(mic_info["distractor"])
            duration = int(mic_info["duration"])
            segments_list.append(
                {
                    "day": day,
                    "stream": "field.audio",
                    "time": f"07{position}{mic_index}00",
                    "duration_seconds": duration,
                    "source": "voices",
                    "source_id": str(mic_info["stem"]),
                    "license": "CC-BY-4.0",
                    "description": (
                        "VOiCES far-field replayed LibriSpeech utterance, "
                        f"rm1 with {DISTRACTOR_PRETTY[distractor]} distractor, "
                        f"{MIC_PRETTY[mic]} mic"
                    ),
                    "exercises": ["transcription", "diarization"],
                    "has_reference": False,
                    "slice": {"start_seconds": 0, "duration_seconds": duration},
                }
            )
    return segments_list
