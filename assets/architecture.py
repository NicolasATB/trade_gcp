"""Render the project architecture diagram — hybrid layout.

Graphviz (via the ``diagrams`` library) lays out the boxes and official GCP
icons; the **arrows are drawn by hand** afterwards so every edge has exact,
predictable routing (left-side exits, straight runs, labels where we want them)
instead of graphviz's auto-router. Icons are inlined as base64 so the output SVG
is self-contained and renders on GitHub.

Pipeline:
    1. Build the 4-layer node/cluster layout with *invisible* ranking edges only.
    2. Render it to SVG (boxes + icons + node labels, no visible arrows).
    3. Parse cluster/node bounding boxes from the SVG, inline the icon PNGs.
    4. Inject hand-routed arrows (paths + arrowheads + labels).
    5. Write assets/architecture.svg.

Regenerate with:
    python assets/architecture.py        # writes assets/architecture.svg

Layering convention (KEEP — standard cloud-architecture style, top→bottom):
    1. Users & external endpoints   2. Application / logic
    3. Data & platform              4. Base infrastructure & IaC (never on top)
"""

from __future__ import annotations

import base64
import os
import re
from pathlib import Path

for _candidate in (r"C:\Program Files\Graphviz\bin", r"C:\Program Files (x86)\Graphviz\bin"):
    if os.path.isdir(_candidate) and _candidate not in os.environ.get("PATH", ""):
        os.environ["PATH"] = _candidate + os.pathsep + os.environ.get("PATH", "")

from diagrams import Cluster, Diagram, Edge, Node  # noqa: E402
from diagrams.gcp.analytics import Bigquery, Dataflow, Looker  # noqa: E402
from diagrams.gcp.storage import GCS  # noqa: E402
from diagrams.onprem.ci import GithubActions  # noqa: E402
from diagrams.onprem.iac import Terraform  # noqa: E402
from diagrams.onprem.network import Internet  # noqa: E402
from diagrams.onprem.vcs import Github  # noqa: E402
from diagrams.onprem.workflow import Airflow  # noqa: E402
from diagrams.programming.language import Python  # noqa: E402
from diagrams.saas.chat import Telegram  # noqa: E402

ASSETS = Path(__file__).parent
LAYOUT_STEM = str(ASSETS / "_architecture_layout")   # temp SVG from graphviz
OUT_SVG = ASSETS / "architecture.svg"

# Palette
BLUE = "#4285F4"
GREEN = "#34A853"
PURPLE = "#A142F4"
GREY = "#9aa0a6"

# margin pads the cluster around its nodes so wide node labels stay inside the box.
CLUSTER_ATTR = {"fontname": "Helvetica-Bold", "fontsize": "22", "margin": "26"}
# offline ML cluster: same look, dashed border (never runs inside the daily DAG)
ML_CLUSTER_ATTR = {**CLUSTER_ATTR, "style": "dashed"}
DASHED_CLUSTERS = {"ml_box"}
NODE_ATTR = {
    "fontname": "Helvetica",
    "fontsize": "20",
    "imagepos": "tc",
    "labelloc": "b",
    "height": "2.4",
}

# ---------------------------------------------------------------------------
# 1) Build the layout (boxes + icons; ranking via invisible edges only)
# ---------------------------------------------------------------------------
INVIS = {"style": "invis"}

with Diagram(
    "",                                # title drawn by hand over the final grid
    filename=LAYOUT_STEM,
    outformat="svg",
    show=False,
    direction="TB",
    graph_attr={
        "fontname": "Helvetica-Bold",
        "fontsize": "34",
        "labelloc": "t",
        "bgcolor": "white",
        "pad": "0.6",
        "nodesep": "1.1",
        "ranksep": "2.7",
        "compound": "true",
    },
    node_attr=NODE_ATTR,
    edge_attr={"fontname": "Helvetica", "fontsize": "10"},
):
    # Rough layout only — exact positions (left-aligned, 80 px gaps) are set by
    # the grid reposition step further down.
    with Cluster("Data sources (input)", graph_attr=CLUSTER_ATTR) as src_box:
        s_fred = Internet("FRED\n(macro)")
        s_yahoo = Internet("Yahoo / Tiingo\n(ETFs)")
        s_binance = Internet("Binance\n(crypto)")
        s_coinmetrics = Internet("Coin Metrics\n(on-chain)")
    with Cluster("User-facing outputs", graph_attr=CLUSTER_ATTR) as out_box:
        telegram = Telegram("Telegram\nalert on change")
        looker = Looker("Looker Studio\nQA dashboard")

    with Cluster("Application / logic", graph_attr=CLUSTER_ATTR) as app_box:
        with Cluster("e2-micro VM — Compute Engine (Airflow)", graph_attr=CLUSTER_ATTR) as vm_box:
            scheduler = Airflow("Airflow\nscheduler")
            ingest = Python("Ingest\nPythonOperator")
            alert = Python("Alert\nPythonOperator")
        with Cluster("Managed Dataflow (Apache Beam)", graph_attr=CLUSTER_ATTR) as df_box:
            df = Dataflow("Dataflow job\nconform · RSI · signal")
        with Cluster("Modeling — offline (research)", graph_attr=ML_CLUSTER_ATTR) as ml_box:
            ml = Python("Backtest engine\n(research, grid, manual run)")

    with Cluster("Data & platform", graph_attr=CLUSTER_ATTR) as data_box:
        gcs = GCS("Cloud Storage\ntemp_location")
        # spacer widens the Data & platform box
        gap2 = Node("", shape="box", style="invis", width="1.4", height="0.1")
        bq = Bigquery("BigQuery — medallion\nbronze → silver → gold")

    with Cluster("Base infrastructure & IaC", graph_attr=CLUSTER_ATTR) as iac_box:
        repo = Github("GitHub")
        ci = GithubActions("CI\nruff + pytest")
        iac = Terraform("Terraform\n(BigQuery + VM)")

    # Invisible ranking edges: pin every node to its layer (top→bottom).
    for n in (s_binance, s_fred, s_coinmetrics, s_yahoo, telegram, looker):
        n >> Edge(**INVIS) >> ingest                 # layer 1 → 2
        n >> Edge(**INVIS) >> ml                      # keep ml in the same rank
    for n in (scheduler, ingest, alert, df, ml):
        n >> Edge(**INVIS) >> bq                      # layer 2 → 3
    for n in (repo, ci, iac):
        bq >> Edge(**INVIS) >> n                      # layer 3 → 4 (GitHub · CI · Terraform)
    for n in (gcs, gap2):
        n >> Edge(**INVIS) >> iac                     # pin Data layer (gap2 = spacer)

# Map logical handles → graphviz ids / cluster names for the SVG parser.
NODE_IDS = {
    "s_binance": s_binance.nodeid, "s_fred": s_fred.nodeid,
    "s_coinmetrics": s_coinmetrics.nodeid, "s_yahoo": s_yahoo.nodeid,
    "telegram": telegram.nodeid, "looker": looker.nodeid,
    "ingest": ingest.nodeid, "alert": alert.nodeid, "scheduler": scheduler.nodeid,
    "df": df.nodeid, "ml": ml.nodeid, "bq": bq.nodeid, "gcs": gcs.nodeid,
    "iac": iac.nodeid, "repo": repo.nodeid, "ci": ci.nodeid,
}
CLUSTER_NAMES = {
    "src_box": src_box.name, "out_box": out_box.name, "app_box": app_box.name,
    "vm_box": vm_box.name, "df_box": df_box.name, "ml_box": ml_box.name,
    "data_box": data_box.name, "iac_box": iac_box.name,
}

# ---------------------------------------------------------------------------
# 2) Parse the layout SVG: bounding boxes + inline the icons
# ---------------------------------------------------------------------------
import html  # noqa: E402

svg = Path(LAYOUT_STEM + ".svg").read_text(encoding="utf-8")

_num = r"-?\d+(?:\.\d+)?"

# Build {title -> bbox} for clusters and {nodeid -> bbox} for nodes by scanning
# the SVG once. Titles are HTML-escaped by graphviz, so un-escape before keying.
_cb_by_title: dict[str, tuple] = {}
for _m in re.finditer(
    # solid clusters render <path ... d="...">; dashed ones (stroke-dasharray)
    # render <polygon ... points="..."> instead — accept either.
    r'<g id="[^"]*" class="cluster">\s*<title>(.*?)</title>\s*'
    r'<(?:path[^>]*\sd|polygon[^>]*\spoints)="([^"]+)"', svg
):
    _nums = [float(x) for x in re.findall(_num, _m.group(2))]
    _xs, _ys = _nums[0::2], _nums[1::2]
    _cb_by_title[html.unescape(_m.group(1))] = (min(_xs), min(_ys), max(_xs), max(_ys))

_nb_by_id: dict[str, tuple] = {}
for _m in re.finditer(
    r'<g id="[^"]*" class="node">\s*<title>(.*?)</title>\s*<image xlink:href="[^"]*"'
    r' width="(' + _num + r')px" height="(' + _num + r')px"[^>]*x="(' + _num
    + r')" y="(' + _num + r')"', svg
):
    _tid, _w, _h, _x, _y = _m.group(1), *(float(g) for g in _m.groups()[1:])
    _nb_by_id[_tid] = (_x, _y, _x + _w, _y + _h)

CB = {k: _cb_by_title[html.unescape(v)] for k, v in CLUSTER_NAMES.items()}
NB = {k: _nb_by_id[v] for k, v in NODE_IDS.items()}

# ---------------------------------------------------------------------------
# 2.5) Reposition the boxes onto an exact grid: left-aligned, 80 px (=60 pt)
# gaps between boxes (vertical between rows, horizontal between side-by-side
# boxes). Graphviz only gives a rough layout; here we set the exact geometry.
# ---------------------------------------------------------------------------
GAP = 60.0                         # 80 px at 96 dpi
APAD = 20.0                        # inner padding of the app box around Dataflow
IAC_XPAD = 40.0                    # extra space left of CI in the IaC box (label breathing room)
NAME2KEY = {v: k for k, v in CLUSTER_NAMES.items()}
NODES_OF = {
    "src_box": ["s_binance", "s_fred", "s_coinmetrics", "s_yahoo"],
    "out_box": ["telegram", "looker"],
    "vm_box": ["scheduler", "ingest", "alert"],
    "df_box": ["df"],
    "ml_box": ["ml"],
    "data_box": ["gcs", "bq"],
    "iac_box": ["iac", "repo", "ci"],
}
_o = dict(CB)                      # original cluster bboxes (xmin, ymin, xmax, ymax)
X0 = _o["src_box"][0]              # common left edge (ymin = top, more negative)

# vertical row shifts (stack rows with GAP between borders)
dy2 = (max(_o["src_box"][3], _o["out_box"][3]) + GAP) - _o["app_box"][1]
dy3 = (_o["app_box"][3] + dy2 + GAP) - _o["data_box"][1]
dy4 = (_o["data_box"][3] + dy3 + GAP) - _o["iac_box"][1]
# horizontal shifts (left-align main boxes; GAP between side-by-side boxes)
dx_app = X0 - _o["app_box"][0]
dx_data = X0 - _o["data_box"][0]
dx_iac = X0 - _o["iac_box"][0]
dx_out = (_o["src_box"][2] + GAP) - _o["out_box"][0]
dx_df = (_o["vm_box"][2] + GAP) - _o["df_box"][0]          # extra shift of df (vm moves with app)
dx_ml = (_o["df_box"][2] + GAP) - _o["ml_box"][0]          # extra shift of ml (df moves with app+df)
app_ext = (_o["ml_box"][2] + dx_app + dx_df + dx_ml + APAD) - (_o["app_box"][2] + dx_app)

CSHIFT = {                          # (dx, dy, extend_right) per cluster
    "src_box": (0.0, 0.0, 0.0), "out_box": (dx_out, 0.0, 0.0),
    "app_box": (dx_app, dy2, app_ext), "vm_box": (dx_app, dy2, 0.0),
    "df_box": (dx_app + dx_df, dy2, 0.0),
    "ml_box": (dx_app + dx_df + dx_ml, dy2, 0.0),
    "data_box": (dx_data, dy3, 60.0),   # widen right so the BigQuery label fits
    "iac_box": (dx_iac, dy4, IAC_XPAD),
}
NSHIFT = {n: CSHIFT[c][:2] for c, ns in NODES_OF.items() for n in ns}


def _shift_node(s, nodeid, dx, dy):
    pat = re.compile(r'(<g id="[^"]*" class="node">\s*<title>' + re.escape(nodeid)
                     + r'</title>)(.*?)(</g>)', re.S)

    def repl(m):
        b = re.sub(r' x="(' + _num + r')"', lambda t: f' x="{float(t.group(1)) + dx:.2f}"', m.group(2))
        b = re.sub(r' y="(' + _num + r')"', lambda t: f' y="{float(t.group(1)) + dy:.2f}"', b)
        return m.group(1) + b + m.group(3)
    return pat.sub(repl, s, count=1)


for _n, (_dx, _dy) in NSHIFT.items():
    if _dx or _dy:
        svg = _shift_node(svg, NODE_IDS[_n], _dx, _dy)

# Enforce a left→right node order inside a box (graphviz's order is unreliable
# for some boxes). Reassign each node to a slot keeping the existing x-centres.
REORDER = {"iac_box": ["repo", "ci", "iac"],
           "vm_box": ["scheduler", "ingest", "alert"]}
for _box, _order in REORDER.items():
    _cur = {n: (NB[n][0] + NB[n][2]) / 2 + NSHIFT[n][0] for n in _order}
    _slots = sorted(_cur.values())
    for _i, _n in enumerate(_order):
        _ddx = _slots[_i] - _cur[_n]
        if abs(_ddx) > 0.5:
            svg = _shift_node(svg, NODE_IDS[_n], _ddx, 0.0)
            NSHIFT[_n] = (NSHIFT[_n][0] + _ddx, NSHIFT[_n][1])

# redraw every cluster as a rounded rect at its final bbox; shift its title text
_cpat = re.compile(r'<g id="[^"]*" class="cluster">\s*<title>(.*?)</title>\s*'
                   r'<(?:path|polygon) fill="([^"]*)" stroke="([^"]*)"[^>]*/>\s*'
                   r'(<text\b[^>]*>.*?</text>)', re.S)


def _redraw(m):
    key = NAME2KEY[html.unescape(m.group(1))]
    fill, stroke = m.group(2), m.group(3)
    ox = _o[key]
    dx, dy, ext = CSHIFT[key]
    x, y = ox[0] + dx, ox[1] + dy
    w, h = (ox[2] - ox[0]) + ext, ox[3] - ox[1]
    dash = ' stroke-dasharray="10,6"' if key in DASHED_CLUSTERS else ""
    rect = (f'<rect x="{x:.2f}" y="{y:.2f}" width="{w:.2f}" height="{h:.2f}" '
            f'rx="10" ry="10" fill="{fill}" stroke="{stroke}"{dash}/>')
    title = re.sub(r'\b(x|y)="(' + _num + r')"',
                   lambda t: f'{t.group(1)}="{float(t.group(2)) + (dx if t.group(1) == "x" else dy):.2f}"',
                   m.group(4), count=2)
    return '<g class="cluster"><title>' + m.group(1) + '</title>' + rect + title
    # NOTE: original </g> stays in place after the match


svg = _cpat.sub(_redraw, svg)

for _key, (dx, dy, ext) in CSHIFT.items():
    b = CB[_key]
    CB[_key] = (b[0] + dx, b[1] + dy, b[2] + dx + ext, b[3] + dy)
for _n, (dx, dy) in NSHIFT.items():
    b = NB[_n]
    NB[_n] = (b[0] + dx, b[1] + dy, b[2] + dx, b[3] + dy)

# Centre the Dataflow icon+label inside its (wider) green box.
_df_dx = (CB["df_box"][0] + CB["df_box"][2]) / 2 - (NB["df"][0] + NB["df"][2]) / 2
svg = _shift_node(svg, NODE_IDS["df"], _df_dx, 0.0)
NB["df"] = (NB["df"][0] + _df_dx, NB["df"][1], NB["df"][2] + _df_dx, NB["df"][3])

# Centre the ML icon+label inside its (wider) dashed box.
_ml_dx = (CB["ml_box"][0] + CB["ml_box"][2]) / 2 - (NB["ml"][0] + NB["ml"][2]) / 2
svg = _shift_node(svg, NODE_IDS["ml"], _ml_dx, 0.0)
NB["ml"] = (NB["ml"][0] + _ml_dx, NB["ml"][1], NB["ml"][2] + _ml_dx, NB["ml"][3])

# Spread CI and Terraform right by IAC_XPAD so the "push/PR" arrow label has
# breathing room between the GitHub icon and the CI icon.
for _n in ("ci", "iac"):
    svg = _shift_node(svg, NODE_IDS[_n], IAC_XPAD, 0.0)
    NB[_n] = (NB[_n][0] + IAC_XPAD, NB[_n][1], NB[_n][2] + IAC_XPAD, NB[_n][3])

# Inline every icon PNG as base64 so the SVG is self-contained.
def _inline(match: re.Match) -> str:
    href = match.group(1)
    data = base64.b64encode(Path(href).read_bytes()).decode("ascii")
    return 'xlink:href="data:image/png;base64,' + data + '"'


svg = re.sub(r'xlink:href="([^"]+\.png)"', _inline, svg)

# ---------------------------------------------------------------------------
# 3) Hand-routed arrows
# ---------------------------------------------------------------------------
# Anchor helpers (ymin = visual top because graphviz uses negative y here).
def L(b):  return (b[0], (b[1] + b[3]) / 2)            # left-mid   # noqa: E704
def R(b):  return (b[2], (b[1] + b[3]) / 2)            # right-mid  # noqa: E704
def T(b):  return ((b[0] + b[2]) / 2, b[1])            # top-mid    # noqa: E704
def B(b):  return ((b[0] + b[2]) / 2, b[3])            # bottom-mid # noqa: E704


def pt(b, fx, fy):
    """Point inside bbox b at fractional position (fx, fy) — (0,0)=top-left."""
    return (b[0] + (b[2] - b[0]) * fx, b[1] + (b[3] - b[1]) * fy)


def EX(cb, nb, side):
    """Point on cluster edge `side`, aligned to node nb's centre (box anchor)."""
    cx, cy = (nb[0] + nb[2]) / 2, (nb[1] + nb[3]) / 2
    return {"t": (cx, cb[1]), "b": (cx, cb[3]),
            "l": (cb[0], cy), "r": (cb[2], cy)}[side]


def velbow(p1, p2, frac=0.5):
    """Vertical-first Z elbow: down/up from p1, across, then into p2."""
    my = p1[1] + (p2[1] - p1[1]) * frac
    if abs(p1[0] - p2[0]) < 1:                 # already aligned → straight
        return [p1, p2]
    return [p1, (p1[0], my), (p2[0], my), p2]


def helbow(p1, p2, frac=0.5):
    """Horizontal-first Z elbow: sideways from p1, vertical, then into p2."""
    mx = p1[0] + (p2[0] - p1[0]) * frac
    if abs(p1[1] - p2[1]) < 1:
        return [p1, p2]
    return [p1, (mx, p1[1]), (mx, p2[1]), p2]


ARROWS: list[dict] = []


def arrow(pts, color, label=None, dashed=False, both=False, ldx=0, ldy=-8, lxy=None):
    ARROWS.append(dict(pts=pts, color=color, label=label, dashed=dashed,
                       both=both, ldx=ldx, ldy=ldy, lxy=lxy))


# Arrows land on cluster BORDERS. Edge labels go in the band BETWEEN the rows
# (lxy) so they never sit on a box. Logic arrows use the inner vm/df boxes.
band12 = (max(CB["src_box"][3], CB["out_box"][3]) + CB["app_box"][1]) / 2
band23 = (CB["app_box"][3] + CB["data_box"][1]) / 2


def _ncx(nb):
    return (nb[0] + nb[2]) / 2


def _dx(f):
    return pt(CB["data_box"], f, 0.0)[0]


# -- data flow (solid) ------------------------------------------------------
_ix, _ax = _ncx(NB["ingest"]), _ncx(NB["alert"])
# download: straight vertical, aligned under Ingest
arrow([(_ix, CB["src_box"][3]), (_ix, EX(CB["vm_box"], NB["ingest"], "t")[1])],
      BLUE, "download", lxy=(_ix, band12))
# raw candles: straight vertical, shifted left of the Ingest column
_raw_x = _ix - 80
arrow([(_raw_x, CB["vm_box"][3]), (_raw_x, CB["data_box"][1])],
      BLUE, "raw candles +\ncontext series", lxy=(_raw_x, band23))
# launch: VM box right → Dataflow box left (label centred in the gap)
arrow([R(CB["vm_box"]), L(CB["df_box"])], BLUE, "launch")
# last signal: straight vertical, shifted left — distinct x from raw candles
_sig_x = _ax - 80
arrow([(_sig_x, CB["data_box"][1]), (_sig_x, CB["vm_box"][3])],
      BLUE, "last signal", lxy=(_sig_x, band23))
# read/write: Dataflow box bottom → Data box RIGHT edge (one bend, double-headed).
# Docks near the TOP of the right edge (fy=0.18) — swapped with the gold-views
# arrow below so the two no longer cross each other.
_df_cx = _ncx(NB["df"])
_rw_dock = pt(CB["data_box"], 1.0, 0.18)
arrow([(_df_cx, CB["df_box"][3]), (_df_cx, _rw_dock[1]), _rw_dock],
      BLUE, "read OHLCV + params /\nwrite RSI + signal", both=True,
      lxy=(_df_cx, _rw_dock[1]), ldy=-16)
# FILE_LOADS: Cloud Storage ↔ BigQuery (intra-box, between the two icons)
arrow([R(NB["gcs"]), L(NB["bq"])], GREY, "FILE_LOADS /\nexport staging",
      dashed=True, both=True)
# push triggers CI: GitHub → GitHub Actions (intra-box, between the two icons).
# Forced horizontal at GitHub's icon centre — the two icons aren't vertically
# aligned (different glyph heights), so a straight R()→L() pair would slant.
_repo_y = R(NB["repo"])[1]
arrow([R(NB["repo"]), (NB["ci"][0], _repo_y)], GREY, "push / PR", dashed=True)
# send only if changed: VM box → Outputs box (up and over the top)
_a = EX(CB["vm_box"], NB["alert"], "t")
_t = EX(CB["out_box"], NB["telegram"], "b")
arrow([_a, (_a[0], T(CB["app_box"])[1] - 30), (_t[0], T(CB["app_box"])[1] - 30), _t],
      GREEN, "send only if changed", lxy=(_a[0] + 0.62 * (_t[0] - _a[0]), band12))
# gold views: Data box right → up into Looker (single bend, turns upward only).
# Docks at the MID height of the right edge — swapped with the read/write arrow
# above so the two no longer cross each other. Looker's centre x sits clear of
# the app/Dataflow boxes, so the vertical run never crosses them.
_lk = EX(CB["out_box"], NB["looker"], "b")     # point under Looker, on the outputs box
_gv = R(CB["data_box"])
arrow([_gv, (_lk[0], _gv[1]), _lk],
      PURPLE, "gold training +\nmonitor views",
      lxy=((_gv[0] + _lk[0]) / 2, _gv[1]), ldy=-14)
# Modeling (offline): Data box right ↔ up into the dashed Modeling box. Docks at
# fy=0.82 on the right edge — below the other two — so the three connections
# stack without crossing. Dashed: a manual research run, never the daily DAG.
# Double-headed: it reads the gold training views and promotes a new versioned
# param row into prod_trade_strategy — the daily job only ever reads that row,
# same "calibrate offline, read-only in prod" contract as the RSI params today.
_ml_cx = _ncx(NB["ml"])
_mv = pt(CB["data_box"], 1.0, 0.82)
arrow([_mv, (_ml_cx, _mv[1]), (_ml_cx, CB["ml_box"][3])],
      GREY, "reads gold training views /\nwrites experiment_runs +\nversioned param row (offline)",
      dashed=True, both=True, lxy=(_ml_cx, _mv[1]), ldy=-16)

# -- provisioning (dashed) — exit the IaC box LEFT, up the FAR-left margin, and
# enter the target boxes' LEFT edge pointing left→right.
_lx = min(CB["data_box"][0], CB["app_box"][0]) - 50
arrow([L(CB["iac_box"]), (_lx, L(CB["iac_box"])[1]),
       (_lx, L(CB["data_box"])[1]), L(CB["data_box"])],
      GREY, "provisions", dashed=True, ldx=0, ldy=-12)
arrow([(_lx, L(CB["data_box"])[1]), (_lx, L(CB["app_box"])[1]), L(CB["app_box"])],
      GREY, None, dashed=True)


# ---------------------------------------------------------------------------
# 4) Emit SVG (markers + paths + labels) and splice into the layout SVG
# ---------------------------------------------------------------------------
def _marker(color: str, start: bool) -> str:
    mid = f"ah-{color.lstrip('#')}-{'s' if start else 'e'}"
    shape = ("M8,0 L0,3 L8,6 Z" if start else "M0,0 L8,3 L0,6 Z")
    return (f'<marker id="{mid}" markerWidth="9" markerHeight="9" refX="'
            + ("0" if start else "8") + '" refY="3" orient="auto" '
            f'markerUnits="strokeWidth"><path d="{shape}" fill="{color}"/></marker>')


def _path(a: dict) -> str:
    d = "M" + " L".join(f"{x:.1f},{y:.1f}" for x, y in a["pts"])
    dash = ' stroke-dasharray="7,6"' if a["dashed"] else ""
    c = a["color"]
    me = f' marker-end="url(#ah-{c.lstrip("#")}-e)"'
    ms = f' marker-start="url(#ah-{c.lstrip("#")}-s)"' if a["both"] else ""
    return (f'<path d="{d}" fill="none" stroke="{c}" stroke-width="3"'
            f'{dash}{me}{ms}/>')


def _label(a: dict) -> str:
    if not a["label"]:
        return ""
    pts = a["pts"]
    if a["lxy"] is not None:                       # explicit label position
        cx, cy = a["lxy"][0] + a["ldx"], a["lxy"][1] + a["ldy"]
    else:                                          # else: longest segment midpoint
        i = max(range(len(pts) - 1),
                key=lambda k: (pts[k + 1][0] - pts[k][0]) ** 2 + (pts[k + 1][1] - pts[k][1]) ** 2)
        (x0, y0), (x1, y1) = pts[i], pts[i + 1]
        cx = (x0 + x1) / 2 + a["ldx"]
        cy = (y0 + y1) / 2 + a["ldy"]
    lines = a["label"].split("\n")
    fs, th = 19, 21.0
    spans = "".join(
        f'<tspan x="{cx:.1f}" dy="{0 if k == 0 else th}">{s}</tspan>'
        for k, s in enumerate(lines)
    )
    return (f'<text x="{cx:.1f}" y="{cy - (len(lines) - 1) * th:.1f}" '
            'text-anchor="middle" font-family="Helvetica,Arial,sans-serif" '
            f'font-size="{fs}" font-weight="normal" fill="#37474f">{spans}</text>')


colors = {BLUE, GREEN, PURPLE, GREY}
defs = "<defs>" + "".join(
    _marker(c, s) for c in colors for s in (False, True)
) + "</defs>"

# Title, drawn centred over the grid, above row 1.
_title_cx = (min(b[0] for b in CB.values()) + max(b[2] for b in CB.values())) / 2
_title_y = min(b[1] for b in CB.values()) - 34
title = (f'<text x="{_title_cx:.1f}" y="{_title_y:.1f}" text-anchor="middle" '
         'font-family="Helvetica-Bold,Arial,sans-serif" font-size="34" font-weight="bold" '
         'fill="#1f2933">Quantitative Trading-Signal Pipeline on GCP</text>')

overlay = (defs + "".join(_path(a) for a in ARROWS)
           + "".join(_label(a) for a in ARROWS) + title)

# Inject just before the last </g> (inside graphviz's transformed group).
idx = svg.rfind("</g>")
svg = svg[:idx] + overlay + svg[idx:]

# Recompute the canvas to fit every box and arrow (boxes were repositioned and
# arrows can extend into the side gutters), with a uniform margin.
MARGIN = 32.0
_xs, _ys = [], []
for _b in CB.values():
    _xs += [_b[0], _b[2]]
    _ys += [_b[1], _b[3]]
for _a in ARROWS:
    for _x, _y in _a["pts"]:
        _xs.append(_x)
        _ys.append(_y)
    if _a["label"]:                              # include label text extents
        if _a["lxy"] is not None:
            _lcx = _a["lxy"][0] + _a["ldx"]
        else:
            _pp = _a["pts"]
            _i = max(range(len(_pp) - 1),
                     key=lambda k: (_pp[k + 1][0] - _pp[k][0]) ** 2 + (_pp[k + 1][1] - _pp[k][1]) ** 2)
            _lcx = (_pp[_i][0] + _pp[_i + 1][0]) / 2 + _a["ldx"]
        _lw = max(len(s) for s in _a["label"].split("\n")) * 11.0
        _xs += [_lcx - _lw / 2, _lcx + _lw / 2]
_ys.append(_title_y - 26)          # title sits above the boxes
_minx, _maxx, _miny, _maxy = min(_xs), max(_xs), min(_ys), max(_ys)
_W, _H = (_maxx - _minx) + 2 * MARGIN, (_maxy - _miny) + 2 * MARGIN
_tx, _ty = MARGIN - _minx, MARGIN - _miny
svg = re.sub(r'width="\d+pt"\s+height="\d+pt"',
             f'width="{_W:.0f}pt" height="{_H:.0f}pt"', svg, count=1)
svg = re.sub(r'viewBox="[^"]*"', f'viewBox="0 0 {_W:.2f} {_H:.2f}"', svg, count=1)
svg = re.sub(r'translate\([^)]*\)', f'translate({_tx:.2f} {_ty:.2f})', svg, count=1)

OUT_SVG.write_text(svg, encoding="utf-8")
Path(LAYOUT_STEM + ".svg").unlink(missing_ok=True)  # drop the temp layout file
print(f"wrote {OUT_SVG}")
