"""Aggregate 10 years of A2 (injury) crash data into compact JSON for the
national page. Streams 3.7 GB of CSVs file-by-file; never holds all in RAM.

Output: taiwan-mobility-atlas/data/a2_aggregates.json (~100–300 KB)
"""
import csv
import json
import re
import sys
import time
from pathlib import Path
from collections import Counter, defaultdict

BASE = Path("/Users/yunching0513/Taitung_Mobility/資料")
OUT  = Path("/Users/yunching0513/Taitung_Mobility/taiwan-mobility-atlas/data/a2_aggregates.json")

# Same naming pattern as A1: 105/106 simple, 107-109 full, 110 simple, 111-114 full.
# A2 file naming differs across years; glob to be safe.
YEARS = [
    (105, 2016, "simple"),
    (106, 2017, "simple"),
    (107, 2018, "full"),
    (108, 2019, "full"),
    (109, 2020, "full"),
    (110, 2021, "simple"),
    (111, 2022, "full"),
    (112, 2023, "full"),
    (113, 2024, "full"),
    (114, 2025, "full"),
]

# ── shared helpers ─────────────────────────────────────────────────
COUNTY_RE = re.compile(
    r"^(?:臺?|台?)(臺北市|台北市|新北市|桃園市|臺中市|台中市|臺南市|台南市|高雄市|"
    r"宜蘭縣|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|嘉義市|新竹市|基隆市|"
    r"屏東縣|花蓮縣|臺東縣|台東縣|澎湖縣|金門縣|連江縣)"
)
DRE_KU  = re.compile(r"^(?:.+?[縣市])(.{1,4}?區)")
DRE_OTH = re.compile(r"^(?:.+?[縣市])(.{1,4}?[鄉鎮市])")
MUNICIPAL = {'新北市','台中市','台南市','高雄市','桃園市','台北市'}

def norm(s):
    return (s or "").replace("臺", "台")

def parse_casualties(s):
    d = i = 0
    if not s: return 0, 0
    m = re.search(r"死亡(\d+)", s); d = int(m.group(1)) if m else 0
    m = re.search(r"受傷(\d+)", s); i = int(m.group(1)) if m else 0
    return d, i

ROC_DT = re.compile(r"(\d+)年(\d+)月(\d+)日\s+(\d+)時(\d+)分(\d+)秒")
def parse_simple_dt(s):
    m = ROC_DT.match(s)
    if not m: return "", "", 0, 0
    roc, mo, day, hh, mm, ss = map(int, m.groups())
    return f"{roc+1911:04d}{mo:02d}{day:02d}", f"{hh:02d}{mm:02d}{ss:02d}", roc+1911, mo

VULN = {"人":0,"慢車":1,"機車":2,"汽車":3,"其他":4}
def classify(s):
    v = s or ""
    if "機車" in v: return "機車"
    if "客車" in v or "貨車" in v or "曳引" in v: return "汽車"
    if "行人" in v or v.strip() == "人": return "人"
    if "自行車" in v or "慢車" in v: return "慢車"
    return "其他"

TYPE_MAP = {
    "車輛本身":"車輛本身","汽(機)車本身":"車輛本身",
    "人與車":"人與車","人與汽(機)車":"人與車",
    "車與車":"車與車","平交道事故":"平交道事故",
}
def norm_type(t):
    return TYPE_MAP.get((t or "").strip(), "其他")

BRACKETS = [
    ("0-14",0,14),("15-24",15,24),("25-44",25,44),
    ("45-64",45,64),("65-74",65,74),("75+",75,200),
]
def bracket(age):
    for lab, lo, hi in BRACKETS:
        if lo <= age <= hi: return lab
    return "—"

def resolve_loc(loc):
    if not loc: return None, None
    cm = COUNTY_RE.match(loc)
    if not cm: return None, None
    cty = norm(cm.group(1))
    if cty in MUNICIPAL:
        dm = DRE_KU.match(loc) or DRE_OTH.match(loc)
    else:
        dm = DRE_OTH.match(loc) or DRE_KU.match(loc)
    dist = norm(dm.group(1)) if dm else None
    if dist and dist.endswith(("市","縣")) and any(c in dist[:-1] for c in "鄉鎮區"):
        m2 = re.search(r"^(.{1,4}?[鄉鎮區])", dist)
        if m2: dist = m2.group(1)
    return cty, dist

# ── accumulators ───────────────────────────────────────────────────
by_year_events     = Counter()   # year → events
by_year_injured    = Counter()   # year → injured (sum, may double-count multi-row per event; we mitigate via dedup)
by_county_year_ev  = defaultdict(Counter)  # county → year → events
by_county_year_inj = defaultdict(Counter)
by_district_ev     = Counter()   # (county, district) → events
by_district_inj    = Counter()
by_age_mode_type   = Counter()   # full-schema only

total_events = 0
total_injured = 0
total_with_coords = 0

start = time.time()
for roc, cy, schema in YEARS:
    dir_path = BASE / f"{roc}年傷亡道路交通事故資料"
    files = sorted(dir_path.glob("*A2*.csv"))
    print(f"\n[{cy}] {schema}  {len(files)} file(s)", file=sys.stderr, flush=True)
    if not files: continue

    # Process each file streaming. Group rows by event-key within a file.
    for fi, f in enumerate(files):
        events_seen = set()  # (date, time, loc) keys for this file
        with open(f, newline='', encoding='utf-8-sig') as fp:
            r = csv.DictReader(fp)
            for row in r:
                loc = norm(row.get("發生地點","") or "")
                cty, dist = resolve_loc(loc)
                if not cty: continue

                if schema == "simple":
                    date_str, time_str, _, _ = parse_simple_dt(row.get("發生時間",""))
                    key = (date_str, time_str, loc[:60])
                else:
                    date_str = row.get("發生日期","")
                    time_str = row.get("發生時間","")
                    key = (date_str, time_str, loc[:60])

                if key in events_seen:
                    # additional party row for same event — no double-counting events
                    continue
                events_seen.add(key)

                deaths, injured = parse_casualties(row.get("死亡受傷人數",""))

                by_year_events[cy]  += 1
                by_year_injured[cy] += injured
                by_county_year_ev[cty][cy]  += 1
                by_county_year_inj[cty][cy] += injured
                if dist:
                    by_district_ev[(cty, dist)]  += 1
                    by_district_inj[(cty, dist)] += injured
                total_events  += 1
                total_injured += injured

                # coords
                try:
                    lon = float(row.get("經度") or 0); lat = float(row.get("緯度") or 0)
                    if lon and lat: total_with_coords += 1
                except (ValueError, TypeError):
                    pass

                # full-schema only: age/mode/type for first-party (we don't keep
                # all parties for memory). For A2 we don't pick "most vulnerable"
                # because we don't reload all parties — instead, the first-row
                # party per event is the principal party (P1).
                if schema == "full":
                    try:
                        age_v = int(row.get("當事者事故發生時年齡","-1"))
                    except (ValueError, TypeError):
                        age_v = -1
                    if 0 <= age_v <= 120:
                        veh = (row.get("當事者區分-類別-大類別名稱-車種","") or "") + \
                              (row.get("當事者區分-類別-子類別名稱-車種","") or "")
                        mode = classify(veh)
                        atyp = norm_type(row.get("事故類型及型態大類別名稱",""))
                        if atyp != "其他":
                            by_age_mode_type[(bracket(age_v), mode, atyp)] += 1
        elapsed = time.time() - start
        print(f"  [{cy}/{fi+1}/{len(files)}] {f.name:<50} events so far: {total_events:>10,}  injured: {total_injured:>10,}  ({elapsed:.0f}s)", file=sys.stderr, flush=True)

# ── serialize ──────────────────────────────────────────────────────
print(f"\n=== DONE  total events: {total_events:,}  injured: {total_injured:,}  ({time.time()-start:.0f}s) ===", file=sys.stderr)

out = {
    "coverage": {
        "years": list(range(2016, 2026)),
        "total_events": total_events,
        "total_injured": total_injured,
        "events_with_coords": total_with_coords,
        "note_zh": "A2 為「事後死亡或受傷」之事故等級；每件事故含多位當事人，本聚合已依（日期+時間+地點）去重至事件層級；2016、2017、2021 為簡式欄位（無年齡與事故態樣細部資料）。",
        "note_en": "A2 = post-24h death or injury events. Multi-party rows are de-duplicated to event level by (date + time + location). 2016, 2017, 2021 use a simplified schema without age/accident-type detail.",
    },
    "by_year_events":     dict(by_year_events),
    "by_year_injured":    dict(by_year_injured),
    "by_county_year": {
        cty: {
            "events":   dict(by_county_year_ev[cty]),
            "injured":  dict(by_county_year_inj[cty]),
        }
        for cty in sorted(by_county_year_ev)
    },
    "by_district_cumulative": {
        f"{c}|{d}": {"events": by_district_ev[(c,d)], "injured": by_district_inj[(c,d)]}
        for (c, d) in sorted(by_district_ev)
    },
    "by_age_mode_type": [
        {"age": k[0], "mode": k[1], "type": k[2], "count": v}
        for k, v in sorted(by_age_mode_type.items(), key=lambda x: -x[1])
    ],
}

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
print(f"\nWrote {OUT}  ({OUT.stat().st_size/1024:.0f} KB)")
