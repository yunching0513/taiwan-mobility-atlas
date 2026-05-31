"""Dissolve district polygons → 22 county boundaries for the national
county choropleth. Uses shapely unary_union to remove internal borders,
then light simplification to keep the file small."""
import json
from collections import defaultdict
from pathlib import Path
from shapely.geometry import shape, mapping
from shapely.ops import unary_union

SRC = Path("/Users/yunching0513/Taitung_Mobility/taiwan-mobility-atlas/districts.geojson")
DST = Path("/Users/yunching0513/Taitung_Mobility/taiwan-mobility-atlas/counties.geojson")

gj = json.loads(SRC.read_text())
by_county = defaultdict(list)
for f in gj["features"]:
    by_county[f["properties"]["COUNTYNAME"]].append(shape(f["geometry"]))

features = []
for county, geoms in by_county.items():
    merged = unary_union(geoms)
    # Simplify ~80m in degrees (~0.0008) — county scale tolerates this well.
    merged = merged.simplify(0.0008, preserve_topology=True)
    features.append({
        "type": "Feature",
        "properties": {"COUNTYNAME": county},
        "geometry": mapping(merged),
    })

out = {"type": "FeatureCollection", "features": features}
DST.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
print(f"Wrote {len(features)} county polygons → {DST.name} ({DST.stat().st_size/1024:.0f} KB)")
for f in sorted(features, key=lambda x: x["properties"]["COUNTYNAME"]):
    gtype = f["geometry"]["type"]
    print(f"  {f['properties']['COUNTYNAME']:<6} {gtype}")
