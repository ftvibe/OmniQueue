"""Generate the OmniQueue pixel octopus logo as SVG.

Run: python tools/make_logo.py
Writes src/omniqueue/static/logo.svg and favicon.svg.

Spherical mantle over a narrower head with eye bulges; eight arms drawn as
smooth tapered splines that end in a spiral curl wrapped around a ball, one
ball colour per data source. Shading is derived automatically.
"""

from __future__ import annotations

import math
from pathlib import Path

W = H = 60
OFF_X, OFF_Y = 2, 1  # centre the 56-unit drawing on the 60-unit canvas
HI, BODY, SHADE, DARK, EYE = "#7fb5b0", "#4f8f8a", "#3b7672", "#26564f", "#0d3b38"
BALLS = ["#e2856c", "#b39a4b", "#a9cbc8", "#c8b47c", "#f3c6b6", "#8fa35a", "#b07a8a", "#fbefdc"]
BALL_HI = "#fbefdc"
BALL_R = 2.4

g: dict[tuple[int, int], str] = {}


def put(x: int, y: int, c: str) -> None:
    x, y = x + OFF_X, y + OFF_Y
    if 0 <= x < W and 0 <= y < H:
        g[(x, y)] = c


def disc(cx: float, cy: float, r: float, c: str) -> None:
    for y in range(int(cy - r - 1), int(cy + r + 2)):
        for x in range(int(cx - r - 1), int(cx + r + 2)):
            if (x + 0.5 - cx) ** 2 + (y + 0.5 - cy) ** 2 <= r * r:
                put(x, y, c)


def empty(x: int, y: int) -> bool:
    """Canvas-coordinate lookup (used by the shading pass)."""
    return g.get((x, y)) is None


# ---- geometry helpers -------------------------------------------------------------------
def catmull_rom(pts: list[tuple[float, float]], per_seg: int = 24) -> list[tuple[float, float]]:
    """Smooth curve through all control points."""
    if len(pts) < 2:
        return pts
    p = [pts[0]] + pts + [pts[-1]]
    out: list[tuple[float, float]] = []
    for i in range(1, len(p) - 2):
        p0, p1, p2, p3 = p[i - 1], p[i], p[i + 1], p[i + 2]
        for k in range(per_seg):
            t = k / per_seg
            t2, t3 = t * t, t * t * t
            x = 0.5 * ((2 * p1[0]) + (-p0[0] + p2[0]) * t + (2 * p0[0] - 5 * p1[0] + 4 * p2[0] - p3[0]) * t2
                       + (-p0[0] + 3 * p1[0] - 3 * p2[0] + p3[0]) * t3)
            y = 0.5 * ((2 * p1[1]) + (-p0[1] + p2[1]) * t + (2 * p0[1] - 5 * p1[1] + 4 * p2[1] - p3[1]) * t2
                       + (-p0[1] + 3 * p1[1] - 3 * p2[1] + p3[1]) * t3)
            out.append((x, y))
    out.append(pts[-1])
    return out


def spiral(start: tuple[float, float], centre: tuple[float, float], turns: float, clockwise: bool,
           r_end: float, steps: int = 90) -> list[tuple[float, float]]:
    """Points spiralling from `start` around `centre`, radius shrinking to r_end."""
    dx, dy = start[0] - centre[0], start[1] - centre[1]
    r0, a0 = math.hypot(dx, dy), math.atan2(dy, dx)
    sign = 1 if clockwise else -1
    pts = []
    for i in range(1, steps + 1):
        t = i / steps
        a = a0 + sign * turns * 2 * math.pi * t
        r = r0 + (r_end - r0) * t
        pts.append((centre[0] + r * math.cos(a), centre[1] + r * math.sin(a)))
    return pts


def draw_arm(path: list[tuple[float, float]], r0: float, r1: float) -> None:
    """Round brush along `path`, radius tapering from r0 to r1."""
    lengths = [0.0]
    for a, b in zip(path, path[1:]):
        lengths.append(lengths[-1] + math.dist(a, b))
    total = lengths[-1] or 1
    for (x, y), L in zip(path, lengths):
        f = L / total
        disc(x, y, r0 + (r1 - r0) * f ** 0.8, BODY)


# ---- head: sphere on top, narrower head below with eye bulges, then the arm crown -------------
disc(28, 15, 12.6, BODY)
for y in range(24, 40):
    hw = {24: 10, 25: 9, 26: 8, 27: 8, 28: 7, 29: 7, 30: 8, 31: 10, 32: 11, 33: 11, 34: 10, 35: 10,
          36: 10, 37: 10, 38: 10, 39: 9}[y]
    for x in range(28 - hw, 28 + hw):
        put(x, y, BODY)

# ---- arms: control points to the curl, then the curl itself around the ball -------------------
# (controls..., curl centre, turns, clockwise)
ARMS = [
    # (controls..., curl centre, turns, clockwise, gap between arm tip and ball)
    ([(23, 34), (15, 29), (9, 21), (8, 14)], (9, 8), 1.1, True, 1.0),        # up-left: full curl
    ([(33, 33), (40, 27), (46, 19), (48, 12)], (46, 5), 0.65, False, 1.4),   # up-right: loose, open hook
    ([(21, 37), (12, 38), (5, 34), (3, 28)], (5, 23), 1.3, True, 0.8),       # left: tight, more than a turn
    ([(35, 37), (44, 38), (51, 34), (53, 28)], (50, 21), 0.55, False, 1.6),  # right: wide shallow hook
    ([(22, 39), (15, 44), (9, 49), (4, 51)], (5, 46), 1.0, False, 1.0),      # down-left outer
    ([(34, 39), (41, 44), (48, 48), (53, 53)], (49, 50), 0.8, True, 1.2),    # down-right outer: small curl
    ([(26, 40), (24, 46), (19, 51), (13, 53)], (16, 49), 0.9, True, 1.1),    # down-left inner
    ([(30, 40), (32, 47), (30, 53), (25, 55)], (30, 51), 1.15, False, 1.0),  # down-right inner: curls under
]
for controls, centre, turns, cw, gap in ARMS:
    body = catmull_rom(controls)
    curl = spiral(controls[-1], centre, turns, cw, BALL_R + gap)
    draw_arm(body + curl, 3.1, 1.0)

# ---- shading ------------------------------------------------------------------------------------
body_px = [(x, y) for (x, y), c in g.items() if c == BODY]


def near_edge(x: int, y: int, reach: int) -> bool:
    return any(empty(x + dx, y + dy) for dx in range(reach + 1) for dy in range(reach + 1) if dx or dy)


dark = {(x, y) for x, y in body_px if near_edge(x, y, 1)}
shade = {(x, y) for x, y in body_px if (x, y) not in dark and near_edge(x, y, 2)}
for p in dark:
    g[p] = DARK
for p in shade:
    g[p] = SHADE
for x, y in body_px:  # highlight rim on the upper-left of the mantle
    if y <= 16 + OFF_Y and x <= 27 + OFF_X and g.get((x, y)) == BODY and any(empty(x - dx, y - dy) for dx, dy in ((1, 0), (0, 1), (1, 1), (2, 0), (0, 2), (2, 1), (1, 2))):
        g[(x, y)] = HI

# ---- eyes: light domes with a dark horizontal slit ----------------------------------------------
EYE_PATTERN = [" hhh ", "hhhhh", "h@@@h", " sss "]
for ex in (20, 31):
    for dy, row in enumerate(EYE_PATTERN):
        for dx, ch in enumerate(row):
            if ch != " ":
                put(ex + dx, 30 + dy, {"h": HI, "@": EYE, "s": SHADE}[ch])

# ---- balls inside the curls --------------------------------------------------------------------
for i, (_, (cx, cy), _, _, _) in enumerate(ARMS):
    disc(cx, cy, BALL_R, BALLS[i])
    put(round(cx - 1.6), round(cy - 1.6), BALL_HI if BALLS[i] != BALL_HI else "#c8b47c")


def svg(background: str | None = None, pad: int = 0) -> str:
    size = W + 2 * pad
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {size} {size}" '
        f'width="{size * 8}" height="{size * 8}" shape-rendering="crispEdges">',
        "<!-- OmniQueue octopus, generated by tools/make_logo.py -->",
    ]
    if background:
        out.append(f'<rect width="{size}" height="{size}" rx="{size / 6:.1f}" fill="{background}"/>')
    for y in range(H):
        x = 0
        while x < W:
            c = g.get((x, y))
            if c is None:
                x += 1
                continue
            run = 1
            while g.get((x + run, y)) == c:
                run += 1
            out.append(f'<rect x="{x + pad}" y="{y + pad}" width="{run}" height="1" fill="{c}"/>')
            x += run
    out.append("</svg>")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    static = Path(__file__).resolve().parent.parent / "src" / "omniqueue" / "static"
    (static / "logo.svg").write_text(svg())
    (static / "favicon.svg").write_text(svg("#073a34", pad=2))
    print("wrote logo.svg and favicon.svg")
