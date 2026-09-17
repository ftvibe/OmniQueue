"""Generate the OmniQueue pixel octopus logo as SVG.

Run: python tools/make_logo.py
Writes src/omniqueue/static/logo.svg and favicon.svg. Tall dome head, eight
chunky arms drawn with a square brush along polylines (blocky staircase look),
two-tone shading derived automatically, and a ball in its own colour at every
arm tip, one per data source.
"""

from __future__ import annotations

import math
from pathlib import Path

W = H = 48
HI, BODY, SHADE, DARK, EYE = "#7fb5b0", "#4f8f8a", "#3b7672", "#26564f", "#0d3b38"
BALLS = ["#e2856c", "#b39a4b", "#a9cbc8", "#c8b47c", "#f3c6b6", "#8fa35a", "#b07a8a", "#fbefdc"]
BALL_HI = "#fbefdc"

g: dict[tuple[int, int], str] = {}


def put(x: int, y: int, c: str) -> None:
    if 0 <= x < W and 0 <= y < H:
        g[(x, y)] = c


def square(cx: float, cy: float, size: int, c: str) -> None:
    x0, y0 = round(cx - size / 2), round(cy - size / 2)
    for y in range(y0, y0 + size):
        for x in range(x0, x0 + size):
            put(x, y, c)


def ellipse(cx: float, cy: float, rx: float, ry: float, c: str) -> None:
    for y in range(H):
        for x in range(W):
            if ((x + 0.5 - cx) / rx) ** 2 + ((y + 0.5 - cy) / ry) ** 2 <= 1:
                put(x, y, c)


def arm(points: list[tuple[float, float]], s0: int, s1: int) -> None:
    """Square brush along a polyline, brush size stepping from s0 down to s1."""
    total = sum(math.dist(a, b) for a, b in zip(points, points[1:]))
    done = 0.0
    for a, b in zip(points, points[1:]):
        seg = math.dist(a, b)
        steps = max(1, int(seg * 2))
        for i in range(steps + 1):
            t = i / steps
            frac = (done + seg * t) / total
            size = s0 if frac < 0.45 else (s1 if frac > 0.8 else (s0 + s1) // 2)
            square(a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, size, BODY)
        done += seg


# ---- head: spherical mantle on top, a narrower head below it, eye bulges at the bottom ----
# half-width of the body per row (centre x = 24)
PROFILE = {
    1: 4, 2: 6, 3: 8, 4: 9, 5: 10, 6: 10, 7: 11, 8: 11, 9: 11, 10: 11, 11: 11, 12: 11, 13: 11,
    14: 10, 15: 10, 16: 9, 17: 8, 18: 7, 19: 7, 20: 6, 21: 6, 22: 6, 23: 6, 24: 7, 25: 8,
    26: 9, 27: 9, 28: 9, 29: 8, 30: 8, 31: 9, 32: 9,
}
for y, hw in PROFILE.items():
    for x in range(24 - hw, 24 + hw):
        put(x, y, BODY)

# ---- arms (asymmetric like a real one) --------------------------------------------------
ARMS = [
    [(19, 26), (12, 21), (8, 14), (7, 8), (9, 4)],             # up-left, curls in at the top
    [(30, 25), (36, 19), (40, 11), (41, 4)],                    # up-right, raised high
    [(18, 30), (10, 31), (4, 28), (3, 21)],                     # left, hooks up
    [(31, 30), (39, 32), (44, 29), (45, 22)],                   # right, hooks up
    [(20, 32), (13, 37), (7, 42), (3, 44)],                     # down-left outer
    [(28, 32), (35, 37), (41, 42), (45, 44)],                   # down-right outer
    [(22, 33), (21, 39), (19, 43), (17, 45)],                   # down-left inner
    [(26, 33), (28, 39), (30, 43), (32, 45)],                   # down-right inner
]
for pts in ARMS:
    arm(pts, 5, 3)

# ---- shading: dark edge and a shade band along lower/right, highlight on the upper-left ----
def empty(x: int, y: int) -> bool:
    return g.get((x, y)) is None


body_px = [(x, y) for (x, y), c in g.items() if c == BODY]
dark = {(x, y) for x, y in body_px if empty(x, y + 1) or empty(x + 1, y) or empty(x + 1, y + 1)}
shade = {
    (x, y) for x, y in body_px
    if (x, y) not in dark and any(
        (x + dx, y + dy) in dark for dx, dy in ((0, 1), (1, 0), (1, 1), (0, 2), (2, 0), (2, 1), (1, 2))
    )
}
for p in dark:
    g[p] = DARK
for p in shade:
    g[p] = SHADE
# highlight: two-pixel rim on the upper-left of the mantle only
for x, y in body_px:
    if y <= 14 and x <= 23 and g.get((x, y)) == BODY and (empty(x - 1, y) or empty(x, y - 1) or empty(x - 1, y - 1) or empty(x - 2, y) or empty(x, y - 2)):
        g[(x, y)] = HI
for x, y in ((22, 4), (23, 4), (21, 5), (20, 6), (19, 7), (18, 8), (17, 10), (17, 11)):
    if g.get((x, y)) == BODY:
        g[(x, y)] = HI

# ---- eyes: two plain squares ---------------------------------------------------------------
for ex in (16, 30):
    for dx in range(2):
        for dy in range(2):
            put(ex + dx, 26 + dy, EYE)


# ---- balls at the arm tips, one colour per source -------------------------------------------
def ball(cx: float, cy: float, colour: str) -> None:
    for y in range(H):
        for x in range(W):
            if (x + 0.5 - cx) ** 2 + (y + 0.5 - cy) ** 2 <= 2.6 ** 2:
                put(x, y, colour)
    put(round(cx - 1.6), round(cy - 1.6), BALL_HI if colour != BALL_HI else "#c8b47c")


for i, pts in enumerate(ARMS):
    (x0, y0), (x1, y1) = pts[-2], pts[-1]
    d = math.dist(pts[-2], pts[-1]) or 1
    cx, cy = x1 + (x1 - x0) / d * 3.2, y1 + (y1 - y0) / d * 3.2
    cx, cy = min(max(cx, 2.6), W - 2.6), min(max(cy, 2.6), H - 2.6)
    ball(cx, cy, BALLS[i])


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
