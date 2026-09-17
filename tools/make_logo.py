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
BALL_R = 2.9

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
    # (controls..., ball centre, turns, clockwise, gap). turns = 0 -> no curl, the ball sits at the tip
    ([(23, 34), (15, 29), (9, 21), (7, 14)], (5, 10), 0.3, True, 1.4),       # up-left: gentle bend at the tip
    ([(33, 33), (40, 27), (45, 19), (47, 12)], (49, 8), 0, False, 0),        # up-right: straight reach
    ([(21, 37), (12, 38), (5, 35), (2, 30)], (2, 26), 0, True, 0),           # left: straight, tip lifts
    ([(35, 37), (43, 38), (50, 34), (53, 28)], (51, 24), 0.35, False, 1.4),  # right: soft hook
    ([(22, 39), (15, 44), (9, 49), (4, 52)], (2, 55), 0, False, 0),          # down-left outer: long sweep
    ([(34, 39), (41, 44), (47, 48), (51, 51)], (54, 53), 0, True, 0),        # down-right outer: trails straight
    ([(26, 40), (24, 46), (20, 51), (15, 54)], (12, 56), 0, True, 0),        # down-left inner: straight
    ([(30, 40), (32, 46), (33, 51), (31, 54)], (29, 53), 0.3, False, 1.4),   # down-right inner: soft hook under
]
for controls, centre, turns, cw, gap in ARMS:
    body = catmull_rom(controls)
    curl = spiral(controls[-1], centre, turns, cw, BALL_R + gap) if turns else []
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
for ex in (22, 29):
    for dy, row in enumerate(EYE_PATTERN):
        for dx, ch in enumerate(row):
            if ch != " ":
                put(ex + dx, 30 + dy, {"h": HI, "@": EYE, "s": SHADE}[ch])

# ---- balls inside the curls --------------------------------------------------------------------
for i, (controls, (cx, cy), turns, _, _) in enumerate(ARMS):
    if not turns:  # straight arm: ball touches the tip
        (x0, y0), (x1, y1) = controls[-2], controls[-1]
        d = math.dist(controls[-2], controls[-1]) or 1
        cx, cy = x1 + (x1 - x0) / d * (BALL_R + 0.6), y1 + (y1 - y0) / d * (BALL_R + 0.6)
        cx = min(max(cx, BALL_R - OFF_X + 0.5), W - OFF_X - BALL_R - 0.5)
        cy = min(max(cy, BALL_R - OFF_Y + 0.5), H - OFF_Y - BALL_R - 0.5)
    colour = BALLS[i]
    disc(cx, cy, BALL_R, colour)
    # shaded lower-right crescent and a highlight pixel top-left
    darker = "#" + "".join(f"{int(int(colour[k:k + 2], 16) * 0.72):02x}" for k in (1, 3, 5))
    for y in range(int(cy - BALL_R - 1), int(cy + BALL_R + 2)):
        for x in range(int(cx - BALL_R - 1), int(cx + BALL_R + 2)):
            dx, dy = x + 0.5 - cx, y + 0.5 - cy
            if dx * dx + dy * dy <= BALL_R ** 2 and dx + dy > BALL_R * 0.9:
                put(x, y, darker)
    put(round(cx - 1.4) - 1, round(cy - 1.4) - 1, BALL_HI if colour != BALL_HI else "#c8b47c")


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
