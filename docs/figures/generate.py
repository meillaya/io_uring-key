#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Generate the architecture scene for the README.

Run from the repository root:

    python3 -B docs/figures/generate.py

The script writes docs/figures/architecture.excalidraw with fixed seeds and
geometry. Render it with the Excalidraw CLI after any layout change.

The palette and defaults mirror agents/excalidraw.nix in the personal nixos
repository: blue for input, purple for the server, amber for the engine, teal
for the I/O backends, red for the Raft caveat, and neutral for data.
"""

from __future__ import annotations

import json
import zlib
from dataclasses import dataclass
from pathlib import Path

HERE = Path(__file__).resolve().parent

PALETTE = {
    "input": ("#1971c2", "#d0ebff"),
    "runtime": ("#9c36b5", "#f3d9fa"),
    "analysis": ("#f08c00", "#fff3bf"),
    "model": ("#0c8599", "#c5f6fa"),
    "risk": ("#e03131", "#ffe3e3"),
    "neutral": ("#495057", "#f1f3f5"),
}
INK = "#1e1e1e"
ARROW_INK = "#495057"
BACKGROUND = "#ffffff"
FONT_BODY = 22
FONT_TITLE = 32
FONT_NOTE = 18


def _seed(element_id: str) -> int:
    return zlib.crc32(element_id.encode()) & 0x7FFFFFFF


def _base(element_id: str, kind: str, x: float, y: float, width: float, height: float) -> dict:
    seed = _seed(element_id)
    return {
        "id": element_id,
        "type": kind,
        "x": x,
        "y": y,
        "width": width,
        "height": height,
        "angle": 0,
        "strokeColor": INK,
        "backgroundColor": "transparent",
        "fillStyle": "hachure",
        "strokeWidth": 2,
        "strokeStyle": "solid",
        "roughness": 2,
        "opacity": 100,
        "groupIds": [],
        "frameId": None,
        "roundness": None,
        "seed": seed,
        "version": 1,
        "versionNonce": seed,
        "isDeleted": False,
        "boundElements": [],
        "updated": 1,
        "link": None,
        "locked": False,
    }


@dataclass(frozen=True)
class Box:
    element_id: str
    x: float
    y: float
    width: float
    height: float
    label: str
    role: str


def box(b: Box) -> tuple[dict, dict]:
    stroke, fill = PALETTE[b.role]
    shape = _base(b.element_id, "rectangle", b.x, b.y, b.width, b.height)
    shape["strokeColor"] = stroke
    shape["backgroundColor"] = fill
    shape["roundness"] = {"type": 3}
    text_id = f"{b.element_id}_label"
    shape["boundElements"] = [{"type": "text", "id": text_id}]

    lines = b.label.split("\n")
    text_height = len(lines) * FONT_BODY * 1.25
    text = _base(
        text_id, "text", b.x + 10, b.y + (b.height - text_height) / 2, b.width - 20, text_height
    )
    text.update(
        {
            "strokeColor": INK,
            "text": b.label,
            "originalText": b.label,
            "fontSize": FONT_BODY,
            "fontFamily": 1,
            "textAlign": "center",
            "verticalAlign": "middle",
            "containerId": b.element_id,
            "lineHeight": 1.25,
            "baseline": FONT_BODY * 0.9,
        }
    )
    return shape, text


def label(
    element_id: str,
    x: float,
    y: float,
    width: float,
    text: str,
    *,
    align: str = "center",
    size: int = FONT_NOTE,
) -> dict:
    height = (text.count("\n") + 1) * size * 1.25
    element = _base(element_id, "text", x, y, width, height)
    element.update(
        {
            "strokeColor": INK,
            "text": text,
            "originalText": text,
            "fontSize": size,
            "fontFamily": 1,
            "textAlign": align,
            "verticalAlign": "top",
            "containerId": None,
            "lineHeight": 1.25,
            "baseline": size * 0.9,
        }
    )
    return element


def title(text: str) -> dict:
    element = _base("title", "text", 40, 30, 1440, 45)
    element.update(
        {
            "strokeColor": INK,
            "text": text,
            "originalText": text,
            "fontSize": FONT_TITLE,
            "fontFamily": 1,
            "textAlign": "left",
            "verticalAlign": "top",
            "containerId": None,
            "lineHeight": 1.25,
            "baseline": FONT_TITLE * 0.9,
        }
    )
    return element


def arrow(element_id: str, x1: float, y1: float, x2: float, y2: float) -> dict:
    left, top = min(x1, x2), min(y1, y2)
    width, height = abs(x2 - x1), abs(y2 - y1)
    element = _base(element_id, "arrow", left, top, width, height)
    element.update(
        {
            "strokeColor": ARROW_INK,
            "points": [[x1 - left, y1 - top], [x2 - left, y2 - top]],
            "lastCommittedPoint": None,
            "startBinding": None,
            "endBinding": None,
            "startArrowhead": None,
            "endArrowhead": "arrow",
            "elbowed": False,
        }
    )
    return element


def scene(elements: list[dict]) -> dict:
    return {
        "type": "excalidraw",
        "version": 2,
        "source": "https://excalidraw.com",
        "elements": elements,
        "appState": {
            "gridSize": None,
            "viewBackgroundColor": BACKGROUND,
        },
        "files": {},
    }


def architecture() -> dict:
    boxes = [
        Box("client", 40, 165, 280, 80, "redis-cli", "input"),
        Box("server", 40, 295, 300, 120, "RESP2 server\nthread per socket", "runtime"),
        Box(
            "engine",
            500,
            255,
            340,
            200,
            "LSM engine\nWAL, memtable\nSSTable, compaction",
            "analysis",
        ),
        Box("io", 980, 300, 190, 110, "I/O backend\ninterface", "neutral"),
        Box("sync", 970, 495, 220, 65, "sync\npread / pwrite", "model"),
        Box("threadpool", 1210, 495, 220, 65, "threadpool\nstd::thread pool", "model"),
        Box("mmap", 970, 575, 220, 65, "mmap\nmapped files", "model"),
        Box("uring", 1210, 575, 220, 65, "io_uring\nbatched I/O", "model"),
        Box("data", 1070, 740, 260, 80, "data directory\nwal.log, sst_*.sst", "neutral"),
        Box("raft", 40, 480, 420, 140, "Raft-lite library\nnot wired into kv_server", "risk"),
    ]

    elements: list[dict] = []
    for b in boxes:
        shape, text = box(b)
        elements.extend([shape, text])

    # The backend container groups the four interchangeable implementations.
    container = _base("backend_set", "rectangle", 950, 450, 500, 250)
    container.update(
        {
            "strokeColor": "#495057",
            "backgroundColor": "transparent",
            "strokeStyle": "dashed",
            "roundness": {"type": 3},
        }
    )
    elements.append(container)
    elements.append(label("backend_set_label", 960, 460, 480, "one of four backends", size=FONT_BODY))

    elements.extend(
        [
            arrow("a_client", 180, 245, 180, 295),
            arrow("a_server", 340, 355, 500, 355),
            arrow("a_engine", 840, 355, 980, 355),
            arrow("a_backend", 1075, 410, 1075, 450),
            arrow("a_data", 1200, 700, 1200, 740),
        ]
    )

    elements.extend(
        [
            label("l_resp", 190, 255, 45, "TCP"),
            label("l_command", 380, 320, 80, "commands"),
            label("l_read", 855, 320, 110, "reads / writes"),
            label("l_backend", 1085, 418, 40, "pick"),
            label("l_files", 1210, 708, 45, "files"),
            label(
                "n_raft",
                40,
                640,
                700,
                "Election, log, snapshots and TCP RPCs exist in src/raft.\n"
                "The tests exercise the library in process; kv_server never calls it.",
                align="left",
            ),
            label(
                "source",
                40,
                855,
                900,
                "Source: src/main.cpp, src/server, src/engine, src/io.",
                align="left",
            ),
        ]
    )

    elements.insert(0, title("kv_server reads and writes through one I/O backend"))
    return scene(elements)


def main() -> None:
    path = HERE / "architecture.excalidraw"
    path.write_text(json.dumps(architecture(), indent=2, sort_keys=True) + "\n")
    print(path)


if __name__ == "__main__":
    main()
