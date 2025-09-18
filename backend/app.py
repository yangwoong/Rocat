
import os, json, math, time
from typing import Optional, List, Dict, Any, Tuple
from fastapi import FastAPI, Body, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2, psycopg2.extras
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.ops import unary_union

DB_URL = os.getenv("DB_URL", "postgresql://postgres:password@localhost:5432/postgres")

def get_conn():
    if not DB_URL:
        raise RuntimeError("DB_URL env not set. Example: postgresql://postgres:postgres@localhost:5432/baegun")
    return psycopg2.connect(DB_URL)

def ensure_postgis():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        conn.commit()

def ensure_schema():
    ensure_postgis()
    with get_conn() as conn, conn.cursor() as cur:
        # boundary
        cur.execute("""
        CREATE TABLE IF NOT EXISTS boundary (
          id SERIAL PRIMARY KEY,
          name TEXT,
          geom geometry(MULTIPOLYGON,4326)
        );
        """)
        # tiles
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tiles (
          id SERIAL PRIMARY KEY,
          tile_id TEXT UNIQUE,
          geom geometry(POLYGON,4326),
          centroid geometry(POINT,4326)
        );
        CREATE INDEX IF NOT EXISTS tiles_gix ON tiles USING GIST (geom);
        """)
        # samples
        cur.execute("""
        CREATE TABLE IF NOT EXISTS samples (
          id BIGSERIAL PRIMARY KEY,
          tile_id TEXT,
          ts TIMESTAMP DEFAULT NOW(),
          ph DOUBLE PRECISION,
          "do" DOUBLE PRECISION,
          chla DOUBLE PRECISION,
          turb DOUBLE PRECISION
        );
        CREATE INDEX IF NOT EXISTS samples_tile_ts_idx ON samples (tile_id, ts DESC);
        """)
        # drones with GPS & video
        cur.execute("""
        CREATE TABLE IF NOT EXISTS drones (
          id TEXT PRIMARY KEY,
          status TEXT,
          battery DOUBLE PRECISION,
          tile_id TEXT,
          lat DOUBLE PRECISION,
          lon DOUBLE PRECISION,
          heading DOUBLE PRECISION,
          video_url TEXT,
          updated_at TIMESTAMP DEFAULT NOW()
        );
        """)
        # missions
        cur.execute("""
        CREATE TABLE IF NOT EXISTS missions (
          id BIGSERIAL PRIMARY KEY,
          ts TIMESTAMP DEFAULT NOW(),
          text TEXT
        );
        """)
        # water_q (수질 값 + LLM 판단 결과 저장)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS water_q (
            idx bigserial PRIMARY KEY,
            zone_id varchar(100) NULL,
            device_id varchar(100) NULL,
            temp_c float8 NULL,
            ph float8 NULL,
            ec_us_cm float8 NULL,
            do_mg_l float8 NULL,
            toc_mg_l float8 NULL,
            cod_mg_l float8 NULL,
            t_n_mg_l float8 NULL,
            t_p_mg_l float8 NULL,
            ss_mg_l float8 NULL,
            cl_mg_l float8 NULL,
            chl_a_mg_m3 float8 NULL,
            cd_mg_l float8 NULL,
            bod_mg_l float8 NULL,
            curr_datetime timestamp DEFAULT now() NULL,
            curr_wq_state varchar(10) NULL,
            target_wq_state varchar(10) NULL,
            reason text NULL,
            reference_sources text NULL
        );
        """)
        conn.commit()

app = FastAPI(title="Baegun Demo Backend v3 (50m grid, GPS, video)")
app.add_middleware(
	CORSMiddleware,
	allow_origins=["*"],
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"],
)

class MissionIn(BaseModel):
    text: str

class SampleIn(BaseModel):
    zone_id: str
    device_id: str
    temp_c: float = 19.5
    ph: float = 7.4
    ec_us_cm: float = 230
    do_mg_l: float = 7.2
    toc_mg_l: float = 2.7
    cod_mg_l: float = 10.6
    t_n_mg_l: float = 1.9
    t_p_mg_l: float = 0.8
    ss_mg_l: float = 5.6
    cl_mg_l: float = 5.6
    chl_a_mg_m3: float = 6.5
    cd_mg_l: float = 0.5
    bod_mg_l: float = 2
    
class WaterQMetrics(BaseModel):
    temp_c: Optional[float] = None
    ph: Optional[float] = None
    ec_us_cm: Optional[float] = None
    do_mg_l: Optional[float] = None
    toc_mg_l: Optional[float] = None
    cod_mg_l: Optional[float] = None
    t_n_mg_l: Optional[float] = None
    t_p_mg_l: Optional[float] = None
    ss_mg_l: Optional[float] = None
    cl_mg_l: Optional[float] = None
    chl_a_mg_m3: Optional[float] = None
    cd_mg_l: Optional[float] = None
    bod_mg_l: Optional[float] = None

class WaterQLLM(BaseModel):
    water_q_idx: Optional[int] = None
    zone_id: Optional[str] = None
    device_id: Optional[str] = None
    curr_wq_state: Optional[str] = None
    target_wq_state: Optional[str] = None
    reason: Optional[str] = None
    reference_sources: Optional[List[str]] = None

class WaterQIn(BaseModel):
    zone_id: str
    device_id: Optional[str] = None
    w_data: WaterQMetrics
    llm: Optional[WaterQLLM] = None  # LLM 판단 결과가 없을 수도 있음
    # ts: Optional[float] = None       # epoch seconds (선택)    

def to_multipolygon(g) -> MultiPolygon:
    if isinstance(g, MultiPolygon): return g
    if isinstance(g, Polygon): return MultiPolygon([g])
    raise ValueError("Boundary must be (Multi)Polygon")

def col_letters(idx: int) -> str:
    s = ""
    idx += 1
    while idx:
        idx, r = divmod(idx-1, 26)
        s = chr(65 + r) + s
    return s

def meters_per_deg(lat_deg: float):
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(math.radians(lat_deg))
    return m_per_deg_lon, m_per_deg_lat

def grid_tiles_for_boundary(boundary_geojson: Dict[str,Any], tile_m: float = 50.0):
    from shapely.geometry import Polygon as SPoly
    g = shape(boundary_geojson)
    mp = to_multipolygon(g)

    lon0, lat0 = mp.centroid.x, mp.centroid.y
    m_per_deg_lon, m_per_deg_lat = meters_per_deg(lat0)

    def ll_to_xy(lon, lat):
        x = (lon - lon0) * (m_per_deg_lon)
        y = (lat - lat0) * (m_per_deg_lat)
        return x, y
    def xy_to_ll(x, y):
        lon = lon0 + x / m_per_deg_lon
        lat = lat0 + y / m_per_deg_lat
        return lon, lat

    minx, miny, maxx, maxy = mp.bounds
    a_x, a_y = ll_to_xy(minx, miny)
    b_x, b_y = ll_to_xy(maxx, maxy)
    x0, y0 = min(a_x,b_x), min(a_y,b_y)
    x1, y1 = max(a_x,b_x), max(a_y,b_y)

    tiles = []
    row = 0
    y = y0
    while y < y1:
        x = x0
        col = 0
        while x < x1:
            square_xy = [(x,y),(x+tile_m,y),(x+tile_m,y+tile_m),(x,y+tile_m),(x,y)]
            square_ll = [xy_to_ll(px,py) for (px,py) in square_xy]
            poly_ll = SPoly(square_ll)
            if mp.intersects(poly_ll):
                tid = f"{col_letters(col)}{row+1}"
                cx, cy = poly_ll.centroid.x, poly_ll.centroid.y
                tiles.append({
                    "tile_id": tid,
                    "polygon": poly_ll,
                    "centroid": (cx, cy)
                })
            x += tile_m; col += 1
        y += tile_m; row += 1
    return tiles

def fetch_boundary_geojson():
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT ST_AsGeoJSON(geom) AS gj FROM boundary ORDER BY id DESC LIMIT 1;")
        row = cur.fetchone()
        if not row or not row["gj"]:
            return None
        return json.loads(row["gj"])

@app.get("/")
def root():
    ensure_schema()
    return {"ok": True, "msg": "baegun backend v3"}

@app.post("/api/boundary/geojson")
def upload_boundary(geojson: Dict[str,Any] = Body(...), name: str = "Baegun Lake"):
    ensure_schema()
    if geojson.get("type") == "FeatureCollection":
        geoms = [ shape(f["geometry"]) for f in geojson.get("features", []) ]
        geom = unary_union(geoms)
    elif geojson.get("type") == "Feature":
        geom = shape(geojson["geometry"])
    else:
        geom = shape(geojson)
    mp = to_multipolygon(geom)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO boundary(name, geom)
            VALUES (%s, ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(%s)),4326));
        """,(name, json.dumps(json.loads(json.dumps(mp.__geo_interface__)))))
        conn.commit()
    return {"ok": True, "stored": "MULTIPOLYGON", "bounds": mp.bounds}

@app.post("/api/tiles/generate")
def generate_tiles(tile_m: float = Query(50.0)):
    ensure_schema()
    bnd = fetch_boundary_geojson()
    if not bnd:
        raise HTTPException(400, "Boundary not set. Upload boundary first.")
    tiles = grid_tiles_for_boundary(bnd, tile_m=tile_m)
    with get_conn() as conn, conn.cursor() as cur:
        for t in tiles:
            gj = json.dumps({
                "type":"Polygon",
                "coordinates":[list(t["polygon"].exterior.coords)]
            })
            cx, cy = t["centroid"]
            cur.execute("""
                INSERT INTO tiles(tile_id, geom, centroid)
                VALUES (%s, ST_SetSRID(ST_GeomFromGeoJSON(%s),4326), ST_SetSRID(ST_Point(%s,%s),4326))
                ON CONFLICT (tile_id) DO UPDATE
                SET geom = EXCLUDED.geom, centroid = EXCLUDED.centroid;
            """,(t["tile_id"], gj, cx, cy))
        conn.commit()
    return {"ok": True, "count": len(tiles)}

@app.get("/api/tiles")
def get_tiles():
    ensure_schema()
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT tile_id, ST_AsGeoJSON(geom) AS gj FROM tiles ORDER BY tile_id;
        """ )
        items = cur.fetchall()
    feats = []
    for it in items:
        gj = json.loads(it["gj"])
        feats.append({
            "type":"Feature",
            "geometry": gj,
            "properties": {"zone_id": it["tile_id"]}
        })
    return {"ok": True, "type":"FeatureCollection", "features": feats}

# @app.post("/api/samples/ingest")
# def ingest_samples(samples: List[SampleIn]):
#     ensure_schema()
#     with get_conn() as conn, conn.cursor() as cur:
#         for s in samples:
#             # ts = s.ts or time.time()  이거 넣으면 에러남... JSON
#             cur.execute("""
#                 INSERT INTO water_q(zone_id, device_id, temp_c, ph, ec_us_cm, do_mg_l, toc_mg_l, cod_mg_l, t_n_mg_l, t_p_mg_l, ss_mg_l, cl_mg_l, chl_a_mg_m3, cd_mg_l, bod_mg_l)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
#             """,(s.zone_id, s.device_id, s.temp_c, s.ph, s.ec_us_cm, s.do_mg_l, s.toc_mg_l, s.cod_mg_l, s.t_n_mg_l, s.t_p_mg_l, s.ss_mg_l, s.cl_mg_l, s.chl_a_mg_m3, s.cd_mg_l, s.bod_mg_l))
#         conn.commit()
#     return {"ok": True, "count": len(samples)}

# @app.get("/api/samples/latest")
# def latest_samples():
#     ensure_schema()
#     with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
#         cur.execute("""
#         SELECT DISTINCT ON (zone_id)
#             idx, zone_id, device_id, temp_c, ph, ec_us_cm, do_mg_l, toc_mg_l, cod_mg_l, t_n_mg_l, 
#             t_p_mg_l, ss_mg_l, cl_mg_l, chl_a_mg_m3, cd_mg_l, bod_mg_l, extract(epoch from curr_datetime), curr_wq_state, target_wq_state 
#         FROM water_q 
#         ORDER BY zone_id, curr_datetime DESC;
#         """)
#         items = cur.fetchall()
#     return {"ok": True, "items": items}

@app.post("/api/wq/ingest")
def wq_ingest(item: WaterQIn):
    ensure_schema()
    # 평문/리스트를 text로 저장 (리스트는 JSON 직렬화)
    ref_sources_text = None
    if item.llm and item.llm.reference_sources:
        import json as _json
        ref_sources_text = _json.dumps(item.llm.reference_sources, ensure_ascii=False)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO water_q (
                zone_id, device_id,
                temp_c, ph, ec_us_cm, do_mg_l, toc_mg_l, cod_mg_l,
                t_n_mg_l, t_p_mg_l, ss_mg_l, cl_mg_l, chl_a_mg_m3, cd_mg_l, bod_mg_l,
                curr_wq_state, target_wq_state, reason, reference_sources
            )
            VALUES (
                %(zone_id)s, %(device_id)s,
                %(temp_c)s, %(ph)s, %(ec_us_cm)s, %(do_mg_l)s, %(toc_mg_l)s, %(cod_mg_l)s,
                %(t_n_mg_l)s, %(t_p_mg_l)s, %(ss_mg_l)s, %(cl_mg_l)s, %(chl_a_mg_m3)s, %(cd_mg_l)s, %(bod_mg_l)s,
                %(curr_wq_state)s, %(target_wq_state)s, %(reason)s, %(reference_sources)s
            )
            RETURNING idx;
        """, {
            "zone_id": item.zone_id,
            "device_id": item.device_id,
            "temp_c": item.w_data.temp_c,
            "ph": item.w_data.ph,
            "ec_us_cm": item.w_data.ec_us_cm,
            "do_mg_l": item.w_data.do_mg_l,
            "toc_mg_l": item.w_data.toc_mg_l,
            "cod_mg_l": item.w_data.cod_mg_l,
            "t_n_mg_l": item.w_data.t_n_mg_l,
            "t_p_mg_l": item.w_data.t_p_mg_l,
            "ss_mg_l": item.w_data.ss_mg_l,
            "cl_mg_l": item.w_data.cl_mg_l,
            "chl_a_mg_m3": item.w_data.chl_a_mg_m3,
            "cd_mg_l": item.w_data.cd_mg_l,
            "bod_mg_l": item.w_data.bod_mg_l,
            "curr_wq_state": item.llm.curr_wq_state if item.llm else None,
            "target_wq_state": item.llm.target_wq_state if item.llm else None,
            "reason": item.llm.reason if item.llm else None,
            "reference_sources": ref_sources_text
        })
        new_id = cur.fetchone()[0]
        conn.commit()
    return {"ok": True, "idx": new_id}

@app.get("/api/samples/latest")  # @app.get("/api/wq/latest")
def wq_latest(zone_id: Optional[str] = None, limit: int = 20):
    ensure_schema()
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        if zone_id:
            cur.execute("SELECT * FROM water_q WHERE zone_id=%s ORDER BY idx DESC LIMIT %s;", (zone_id, limit))
        else:
            cur.execute("SELECT * FROM water_q ORDER BY idx DESC LIMIT %s;", (limit,))
        rows = cur.fetchall()
    return {"ok": True, "items": rows}

@app.get("/api/drones")
def drones_get():
    ensure_schema()
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT id, status, battery, tile_id, lat, lon, heading, video_url FROM drones;")
        items = cur.fetchall()
    drones = { it["id"]: {"status":it["status"],"battery":it["battery"],"tile_id":it["tile_id"],
                          "lat":it["lat"],"lon":it["lon"],"heading":it["heading"],"video_url":it["video_url"]} for it in items }
    return {"ok": True, "drones": drones}

@app.post("/api/drones")
def drones_post(drone: Dict[str, Any]):
    ensure_schema()
    did = drone.get("id","Roboat_1")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO drones(id, status, battery, tile_id, lat, lon, heading, video_url, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE
            SET status=EXCLUDED.status, battery=EXCLUDED.battery, tile_id=EXCLUDED.tile_id,
                lat=EXCLUDED.lat, lon=EXCLUDED.lon, heading=EXCLUDED.heading,
                video_url=EXCLUDED.video_url, updated_at=NOW();
        """,(did, drone.get("status","IDLE"), float(drone.get("battery",100)), drone.get("tile_id"),
             drone.get("lat"), drone.get("lon"), drone.get("heading"), drone.get("video_url")))
        conn.commit()
    return {"ok": True}   

class MissionIn(BaseModel):
    text: str

@app.post("/api/missions/chat")
def mission_chat(m: MissionIn):
    
    # LLM
    ensure_schema()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO missions(mission_id, text, link_mission_id, zone_id, lat, lon, curr_wq_state, target_wq_state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""", 
        (m.get("mission_id"), m.get("text"), m.get("link_mission_id"), m.get("zone_id"), m.get("lat"), m.get("lon"), m.get("curr_wq_state"), m.get("target_wq_state")) )
        conn.commit()

    # m.text = """{
    #         "mission_id": 40,        
    #         "link_mission_id": 0,    
    #         "zone_id": "P18",         
    #         "lat": "37.382889"        
    #         "lon": "127.002551"     
    #         "curr_wq_state": "IV",   
    #         "target_wq_state": "III",  
    #         "response": "P18구역 수행임무 입니다. ** 목표 수질단계는 'III' 보통 이며 하위목표로 용존산소량(do_mg_l)가 8.0이상, 수소이온농도(ph)는 7.0으로 합니다. 참여드론은 wamv1번이며, 구역에서 SPIRAL로 장비는 SPRAY를 사용합니다. 바람이 다소강할 수 있으니 주의 해야 합니다."
    #         }"""
            
        
    return {"RES": True, "mission_id": m.mission_id, "link_mission_id": m.link_mission_id, "zone_id": m.zone_id, "lat": m.lat, "lon": m.lon, "curr_wq_state": m.curr_wq_state, "target_wq_state": m.target_wq_state, "response": "P18구역 수행임무 입니다. ** 목표 수질단계는 'III' 보통 이며 하위목표로 용존산소량(do_mg_l)가 8.0이상, 수소이온농도(ph)는 7.0으로 합니다. 참여드론은 wamv1번이며, 구역에서 SPIRAL로 장비는 SPRAY를 사용합니다. 바람이 다소강할 수 있으니 주의 해야 합니다." }

# --- 1) tile_id → centroid 좌표 ---
@app.get("/api/tiles/centroid")
def tile_to_coord(tile_id: str = Query(..., description="타일 ID (예: C7)")):
    ensure_schema()
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT ST_X(centroid) AS lon, ST_Y(centroid) AS lat
            FROM tiles WHERE tile_id = %s;
        """, (tile_id))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, detail="Tile not found")
        return {"zone_id": tile_id, "lat": row["lat"], "lon": row["lon"]}

# --- 2) GPS 좌표 → tile_id ---
@app.get("/api/tiles/locate")
def coord_to_tile(lat: float, lon: float):
    ensure_schema()
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT tile_id
            FROM tiles
            WHERE ST_Contains(geom, ST_SetSRID(ST_Point(%s,%s),4326));
        """, (lon, lat))  # WKT: (lon,lat) 순서
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, detail="No tile found for this coordinate")
        return {"lat": lat, "lon": lon, "tile_id": row["tile_id"]}
