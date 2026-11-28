import csv
import io
import json
import logging
import math
import os
import sys
import threading
import time
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

STATIC_ZIP_PATH = BASE_DIR / "Bus-20261101.zip"
LOCAL_PB_PATH = BASE_DIR / "YokohamaMunicipalBus_trip_update"

# 横浜市周辺の範囲
LAT_MIN, LAT_MAX = 35.31, 35.60
LON_MIN, LON_MAX = 139.46, 139.70

# 所要時間計算パラメータ（迂回係数1.25、徒歩分速80m、自転車分速250m＝時速15km相当）
DETOUR_FACTOR = 1.25
WALK_SPEED_M_PER_MIN = 80.0
BIKE_SPEED_M_PER_MIN = 250.0
DOWNSTREAM_MIN_DIST_M = 500.0
DOWNSTREAM_MAX_DIST_M = 5000.0
MAX_DELAY_SEC = 5400  # 90分超の遅延は車載器未更新等の異常値として除外

GBFS_PROVIDERS = {
    "hellocycling": {
        "label": "HELLO CYCLING",
        "info_url": "https://api-public.odpt.org/api/v4/gbfs/hellocycling/station_information.json",
        "status_url": "https://api-public.odpt.org/api/v4/gbfs/hellocycling/station_status.json",
    },
    "docomo-cycle": {
        "label": "ドコモ・バイクシェア（NOLL）",
        "info_url": "https://api-public.odpt.org/api/v4/gbfs/docomo-cycle/station_information.json",
        "status_url": "https://api-public.odpt.org/api/v4/gbfs/docomo-cycle/station_status.json",
    },
}

JST = timezone(timedelta(hours=9))

# 静的GTFSのメモリキャッシュ
STOPS = {}
ROUTES = {}
TRIP_TO_ROUTE = {}
TRIP_HEADSIGN = {}
STOP_TIMES = {}
GBFS_INFO_CACHE = {}
BIKESHARE_CACHE = {"stations": None, "fetched_at": 0.0}
BIKESHARE_CACHE_TTL_SEC = 30.0
BIKESHARE_CACHE_LOCK = threading.Lock()


def parse_gtfs_time_seconds(time_str: str) -> int | None:
    """GTFSの時刻文字列（24時超え表記を含む HH:MM:SS）を経過秒数へ変換する。"""
    parts = (time_str or "").strip().split(":")
    if len(parts) != 3:
        return None
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
    except ValueError:
        return None
    if hours < 0 or not (0 <= minutes < 60) or not (0 <= seconds < 60):
        return None
    return hours * 3600 + minutes * 60 + seconds


def resolve_gtfs_service_timestamp(
    sched_secs: int, ref_ts: int, start_date_str: str = ""
) -> int:
    """運行日（start_date または基準時刻に最も近い営業日）から定刻のUnixタイムスタンプを算出する。"""
    clean_date = (start_date_str or "").strip()
    if len(clean_date) == 8 and clean_date.isdigit():
        try:
            service_dt = datetime.strptime(clean_date, "%Y%m%d").replace(tzinfo=JST)
            return int(service_dt.timestamp()) + sched_secs
        except ValueError:
            pass

    ref_dt = datetime.fromtimestamp(ref_ts, JST)
    base_midnight = datetime(ref_dt.year, ref_dt.month, ref_dt.day, tzinfo=JST)
    candidates = [
        int((base_midnight + timedelta(days=offset)).timestamp()) + sched_secs
        for offset in (-1, 0, 1)
    ]
    return min(candidates, key=lambda cand: abs(cand - ref_ts))


def load_static_gtfs():
    with zipfile.ZipFile(STATIC_ZIP_PATH, "r") as zf:
        with zf.open("stops.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                sid = row.get("stop_id")
                lat_str = row.get("stop_lat")
                lon_str = row.get("stop_lon")
                if not sid or not lat_str or not lon_str:
                    continue
                try:
                    lat, lon = float(lat_str), float(lon_str)
                except ValueError:
                    continue
                sid = sys.intern(sid)
                STOPS[sid] = {
                    "stop_id": sid,
                    "name": row.get("stop_name") or sid,
                    "lat": lat,
                    "lon": lon,
                }
        with zf.open("routes.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                rid = row.get("route_id")
                if not rid:
                    continue
                ROUTES[rid] = (
                    row.get("route_short_name")
                    or row.get("route_long_name")
                    or rid
                )
        with zf.open("trips.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                tid = row.get("trip_id")
                rid = row.get("route_id")
                if tid and rid:
                    tid = sys.intern(tid)
                    TRIP_TO_ROUTE[tid] = rid
                    TRIP_HEADSIGN[tid] = (row.get("trip_headsign") or "").strip()
        with zf.open("stop_times.txt") as f:
            for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                tid = row.get("trip_id")
                sid = row.get("stop_id")
                if not tid or not sid:
                    continue
                secs = parse_gtfs_time_seconds(
                    row.get("arrival_time") or row.get("departure_time") or ""
                )
                if secs is None:
                    continue
                key = (sys.intern(tid), sys.intern(sid))
                if key not in STOP_TIMES:
                    STOP_TIMES[key] = secs


@asynccontextmanager
async def lifespan(_app: FastAPI):
    load_static_gtfs()
    yield


app = FastAPI(lifespan=lifespan)


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def estimate_walk_minutes(distance_m: float) -> int:
    return max(1, math.ceil((distance_m * DETOUR_FACTOR) / WALK_SPEED_M_PER_MIN))


def select_representative_downstream_stops(candidates: list[dict]) -> list[dict]:
    """500m〜5000m内の下流バス停リストから終点および途中の主要・等間隔バス停を最大3件選出する。"""
    n = len(candidates)
    if n <= 3:
        return candidates

    last_idx = n - 1
    idx1 = max(0, min(last_idx - 2, round(last_idx / 3)))
    idx2 = max(idx1 + 1, min(last_idx - 1, round(2 * last_idx / 3)))

    # 近傍（±1停留所）に「駅」を含む主要バス停があれば優先する
    for offset in (0, -1, 1):
        cand = idx1 + offset
        if 0 <= cand < idx2 and "駅" in candidates[cand]["stop_name"]:
            idx1 = cand
            break

    for offset in (0, 1, -1):
        cand = idx2 + offset
        if idx1 < cand < last_idx and "駅" in candidates[cand]["stop_name"]:
            idx2 = cand
            break

    return [candidates[idx1], candidates[idx2], candidates[last_idx]]


def load_gtfs_rt_feed(use_realtime: bool = True):
    load_dotenv(BASE_DIR / ".env")
    gtfs_rt_url = os.environ.get("YOKOHAMA_GTFS_RT_URL", "").strip()
    odpt_api_key = os.environ.get("ODPT_API_KEY", "").strip()
    realtime_configured = bool(gtfs_rt_url)

    if use_realtime:
        if gtfs_rt_url:
            try:
                params = {"acl:consumerKey": odpt_api_key} if odpt_api_key else None
                res = requests.get(gtfs_rt_url, params=params, timeout=20)
                res.raise_for_status()
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(res.content)
                return feed, "realtime_api", None, realtime_configured
            except requests.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else "error"
                reason = f"API HTTP {status_code}"
                print(f"GTFS-RT API取得エラー ({reason}) - ローカルへフォールバックします")
            except Exception as e:
                reason = f"API接続/解析エラー ({type(e).__name__})"
                print(f"GTFS-RT API取得エラー ({e}) - ローカルへフォールバックします")

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(LOCAL_PB_PATH.read_bytes())
            return feed, "local_fallback", reason, realtime_configured
        else:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(LOCAL_PB_PATH.read_bytes())
            return feed, "local_fallback", "環境変数未設定", realtime_configured

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(LOCAL_PB_PATH.read_bytes())
    return feed, "local_file", None, realtime_configured


def extract_rental_url(station: dict) -> str | None:
    uris = station.get("rental_uris")
    if not isinstance(uris, dict):
        return None
    for key in ("web", "ios", "android"):
        val = uris.get(key)
        if isinstance(val, str):
            cleaned = val.strip()
            if cleaned.startswith(("https://", "http://")):
                return cleaned
    return None


def _parse_gbfs_flag(val, default: bool = True) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "yes")


def fetch_bikeshare_stations(force_refresh: bool = False):
    now = time.monotonic()
    if not force_refresh:
        with BIKESHARE_CACHE_LOCK:
            cached = BIKESHARE_CACHE["stations"]
            if cached is not None and (now - BIKESHARE_CACHE["fetched_at"]) < BIKESHARE_CACHE_TTL_SEC:
                return cached

    all_stations = []
    for key, cfg in GBFS_PROVIDERS.items():
        try:
            if key not in GBFS_INFO_CACHE:
                info_res = requests.get(cfg["info_url"], timeout=20)
                info_res.raise_for_status()
                stations = {}
                raw_info_stations = (info_res.json().get("data") or {}).get("stations") or []
                for st in raw_info_stations:
                    if not st:
                        continue
                    sid = st.get("station_id")
                    lat, lon = st.get("lat"), st.get("lon")
                    if not sid or lat is None or lon is None:
                        continue
                    try:
                        lat_f, lon_f = float(lat), float(lon)
                    except (TypeError, ValueError):
                        continue
                    if LAT_MIN <= lat_f <= LAT_MAX and LON_MIN <= lon_f <= LON_MAX:
                        stations[sid] = {
                            "station_id": sid,
                            "provider": key,
                            "provider_label": cfg["label"],
                            "name": st.get("name") or "",
                            "lat": lat_f,
                            "lon": lon_f,
                            "rental_url": extract_rental_url(st),
                        }
                GBFS_INFO_CACHE[key] = stations

            status_res = requests.get(cfg["status_url"], timeout=20)
            status_res.raise_for_status()
            info_map = GBFS_INFO_CACHE[key]

            raw_status_stations = (status_res.json().get("data") or {}).get("stations") or []
            for st in raw_status_stations:
                if not st:
                    continue
                sid = st.get("station_id")
                if sid in info_map:
                    base = info_map[sid]
                    bikes_val = st.get("num_bikes_available")
                    docks_val = st.get("num_docks_available")
                    is_renting = _parse_gbfs_flag(st.get("is_renting"), True)
                    is_returning = _parse_gbfs_flag(st.get("is_returning"), True)
                    all_stations.append(
                        {
                            **base,
                            "bikes": int(bikes_val) if (bikes_val is not None and is_renting) else 0,
                            "docks": int(docks_val) if (docks_val is not None and is_returning) else 0,
                            "is_renting": is_renting,
                            "is_returning": is_returning,
                        }
                    )
        except Exception as e:
            print(f"GBFS取得エラー ({key}): {e}")
    if all_stations:
        with BIKESHARE_CACHE_LOCK:
            BIKESHARE_CACHE["stations"] = all_stations
            BIKESHARE_CACHE["fetched_at"] = time.monotonic()
    return all_stations


def find_ports_near_coords(s_lat: float, s_lon: float, bikes: list[dict]) -> list[tuple[float, dict]]:
    """指定座標から半径300m以内のシェアサイクルポートを距離昇順で返す。"""
    near_list = []
    for b in bikes:
        if abs(b["lat"] - s_lat) > 0.004 or abs(b["lon"] - s_lon) > 0.004:
            continue
        dist_m = haversine_m(s_lat, s_lon, b["lat"], b["lon"])
        if dist_m <= 300:
            near_list.append((dist_m, b))
    near_list.sort(key=lambda x: x[0])
    return near_list


def collect_top_available_ports(near_ports: list[tuple[float, dict]], limit: int = 3) -> list[dict]:
    """半径300m以内の空き1台以上ポートを距離順・台数順で上位limit件返す。"""
    available_candidates = []
    for dist_m, b in near_ports:
        if b["bikes"] >= 1:
            port_entry = {
                "station_id": b["station_id"],
                "name": b["name"] or "名称未設定",
                "provider": b["provider"],
                "provider_label": b["provider_label"],
                "bikes": b["bikes"],
                "docks": b["docks"],
                "lat": b["lat"],
                "lon": b["lon"],
                "rental_url": b.get("rental_url"),
                "distance_m": int(round(dist_m)),
                "raw_distance_m": dist_m,
                "walk_min": estimate_walk_minutes(dist_m),
            }
            available_candidates.append((dist_m, -b["bikes"], port_entry))
    available_candidates.sort(key=lambda x: (x[0], x[1]))
    return [c[2] for c in available_candidates[:limit]]


@app.get("/")
def index():
    return FileResponse(
        BASE_DIR / "index.html",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/api/map-data")
def get_map_data(
    min_delay_min: int = Query(5, ge=1, le=60),
    use_realtime: bool = Query(True),
):
    if not STOPS or not STOP_TIMES:
        load_static_gtfs()

    feed, source, fallback_reason, realtime_configured = load_gtfs_rt_feed(
        use_realtime=use_realtime
    )

    feed_ts = feed.header.timestamp or int(time.time())
    current_ts = int(time.time()) if source == "realtime_api" else feed_ts
    min_delay_sec = min_delay_min * 60

    # 基準時刻から前後（-5分〜+30分）に到着予定の遅延便をバス停ごとに集計し、下流停留所候補・到着予測も記録
    stop_delays = {}
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        tid = tu.trip.trip_id
        start_date_str = tu.trip.start_date
        rid = TRIP_TO_ROUTE.get(tid) or ""
        rname = ROUTES.get(rid) or "不明系統"
        headsign = TRIP_HEADSIGN.get(tid) or ""
        stu_list = list(tu.stop_time_update)

        for idx, stu in enumerate(stu_list):
            delay = max(stu.arrival.delay, stu.departure.delay)
            if not (min_delay_sec <= delay <= MAX_DELAY_SEC):
                continue

            sid = stu.stop_id
            if sid not in STOPS:
                continue

            arr_ts = stu.arrival.time or stu.departure.time
            sched_secs = STOP_TIMES.get((tid, sid))
            if sched_secs is not None:
                scheduled_ts = resolve_gtfs_service_timestamp(
                    sched_secs, arr_ts or feed_ts, start_date_str
                )
            elif arr_ts:
                scheduled_ts = arr_ts - delay
            else:
                scheduled_ts = None

            predicted_ts = (
                arr_ts
                if arr_ts
                else (scheduled_ts + delay if scheduled_ts is not None else None)
            )
            if predicted_ts is not None and not (
                feed_ts - 300 <= predicted_ts <= feed_ts + 1800
            ):
                continue

            orig_stop = STOPS[sid]
            entry = stop_delays.setdefault(
                sid,
                {
                    **orig_stop,
                    "max_delay_sec": 0,
                    "routes": set(),
                    "departing_headsigns": set(),
                    "all_headsigns": set(),
                    "delayed_trips": 0,
                    "arrivals": [],
                    "best_trip": None,
                },
            )
            if delay > entry["max_delay_sec"]:
                entry["max_delay_sec"] = delay
            entry["routes"].add(rname)
            clean_hs = headsign.removesuffix("行").strip()
            if clean_hs:
                entry["all_headsigns"].add(clean_hs)
                if idx < len(stu_list) - 1:
                    entry["departing_headsigns"].add(clean_hs)
            entry["delayed_trips"] += 1

            if predicted_ts is not None:
                remaining_sec = predicted_ts - current_ts
                if remaining_sec >= -60:
                    remaining_min = (
                        0 if remaining_sec <= 0 else math.ceil(remaining_sec / 60)
                    )
                    predicted_time_str = datetime.fromtimestamp(
                        predicted_ts, JST
                    ).strftime("%H:%M")
                    scheduled_time_str = (
                        datetime.fromtimestamp(scheduled_ts, JST).strftime("%H:%M")
                        if scheduled_ts is not None
                        else ""
                    )
                    entry["arrivals"].append(
                        {
                            "trip_id": tid,
                            "route_name": rname,
                            "headsign": headsign,
                            "delay_min": round(delay / 60, 1),
                            "remaining_min": remaining_min,
                            "predicted_time_str": predicted_time_str,
                            "scheduled_time_str": scheduled_time_str,
                            "_predicted_ts": predicted_ts,
                        }
                    )

            # この便における当該バス停より後の下流バス停（直線500m〜5000m）を抽出
            downstream_in_range = []
            seen_dst = {sid}
            for dst_stu in stu_list[idx + 1 :]:
                dst_sid = dst_stu.stop_id
                if dst_sid in seen_dst or dst_sid not in STOPS:
                    continue
                dst_ts = dst_stu.arrival.time or dst_stu.departure.time
                if not dst_ts or dst_ts <= feed_ts:
                    continue
                dst_stop = STOPS[dst_sid]
                straight_m = haversine_m(
                    orig_stop["lat"],
                    orig_stop["lon"],
                    dst_stop["lat"],
                    dst_stop["lon"],
                )
                if DOWNSTREAM_MIN_DIST_M <= straight_m <= DOWNSTREAM_MAX_DIST_M:
                    seen_dst.add(dst_sid)
                    downstream_in_range.append(
                        {
                            "stop_id": dst_sid,
                            "stop_name": dst_stop["name"],
                            "lat": dst_stop["lat"],
                            "lon": dst_stop["lon"],
                            "straight_distance_m": int(round(straight_m)),
                            "arrival_ts": dst_ts,
                        }
                    )

            # 下流候補を持つ便を優先し、その中でも遅延が大きい便を比較代表便として保持
            has_downstream = 1 if downstream_in_range else 0
            cand_rank = (has_downstream, delay, -(predicted_ts or feed_ts))
            prev_best = entry["best_trip"]
            if prev_best is None or cand_rank > prev_best["rank"]:
                entry["best_trip"] = {
                    "rank": cand_rank,
                    "trip_id": tid,
                    "route_name": rname,
                    "headsign": headsign,
                    "delay_min": round(delay / 60, 1),
                    "downstream_candidates": downstream_in_range,
                }

    bikes = fetch_bikeshare_stations(force_refresh=True)

    # 任意のバス停から半径300m以内のポート一覧をキャッシュ付きで取得するヘルパー
    stop_ports_cache = {}

    def get_ports_near_stop(sid: str):
        if sid in stop_ports_cache:
            return stop_ports_cache[sid]
        st_info = STOPS[sid]
        near_list = find_ports_near_coords(st_info["lat"], st_info["lon"], bikes)
        stop_ports_cache[sid] = near_list
        return near_list

    def build_comparisons_for_origin_port(orig_p: dict, selected_dst: list[dict]) -> list[dict]:
        """指定された貸出ポートを出発地として各代表下流バス停との所要時間比較を算出する。"""
        prov_key = orig_p["provider"]
        walk1_raw = (orig_p["raw_distance_m"] * DETOUR_FACTOR) / WALK_SPEED_M_PER_MIN
        walk_to_port_min = max(1, math.ceil(walk1_raw))
        results = []

        for dst in selected_dst:
            bus_eta_min = max(1, math.ceil((dst["arrival_ts"] - feed_ts) / 60))
            dst_near_ports = get_ports_near_stop(dst["stop_id"])
            same_prov_dst_ports = [
                (d_m, p) for d_m, p in dst_near_ports if p["provider"] == prov_key
            ]
            total_docks_300m = sum(p["docks"] for _, p in same_prov_dst_ports)
            returnable_dst_ports = [
                (d_m, p) for d_m, p in same_prov_dst_ports if p["docks"] >= 1
            ]

            if not returnable_dst_ports:
                results.append(
                    {
                        "stop_id": dst["stop_id"],
                        "stop_name": dst["stop_name"],
                        "straight_distance_m": dst["straight_distance_m"],
                        "bus_eta_min": bus_eta_min,
                        "returnable": False,
                        "provider": prov_key,
                        "provider_label": orig_p["provider_label"],
                        "origin_port_name": orig_p["name"],
                        "origin_station_id": orig_p["station_id"],
                        "bike_total_min": None,
                        "time_diff_min": None,
                        "dest_docks_300m": 0,
                        "dest_dock_low_risk": True,
                        "dest_port_name": None,
                        "dest_port_docks": 0,
                        "dest_port_distance_m": None,
                    }
                )
                continue

            dst_port_dist_m, dst_p = returnable_dst_ports[0]
            ride_straight_m = haversine_m(
                orig_p["lat"], orig_p["lon"], dst_p["lat"], dst_p["lon"]
            )
            ride_raw = (ride_straight_m * DETOUR_FACTOR) / BIKE_SPEED_M_PER_MIN
            walk2_raw = (dst_port_dist_m * DETOUR_FACTOR) / WALK_SPEED_M_PER_MIN
            bike_total_min = max(1, math.ceil(walk1_raw + ride_raw + walk2_raw))

            results.append(
                {
                    "stop_id": dst["stop_id"],
                    "stop_name": dst["stop_name"],
                    "straight_distance_m": dst["straight_distance_m"],
                    "bus_eta_min": bus_eta_min,
                    "time_diff_min": bus_eta_min - bike_total_min,
                    "returnable": True,
                    "provider": prov_key,
                    "provider_label": orig_p["provider_label"],
                    "origin_port_name": orig_p["name"],
                    "origin_station_id": orig_p["station_id"],
                    "bike_total_min": bike_total_min,
                    "walk_to_port_min": walk_to_port_min,
                    "ride_min": max(1, math.ceil(ride_raw)),
                    "walk_from_port_min": max(1, math.ceil(walk2_raw)),
                    "dest_docks_300m": total_docks_300m,
                    "dest_dock_low_risk": total_docks_300m <= 2,
                    "dest_port_name": dst_p["name"] or "名称未設定",
                    "dest_port_docks": dst_p["docks"],
                    "dest_port_distance_m": int(round(dst_port_dist_m)),
                }
            )

        return results

    # 各遅延バス停から半径300m以内のポート・空き状況、最寄り上位3ポート、下流バス停との所要時間比較を算出
    delayed_stops = []
    avoidable_count = 0
    out_of_stock_count = 0
    no_port_count = 0

    for item in stop_delays.values():
        sid = item["stop_id"]
        near_ports = get_ports_near_stop(sid)
        nearby_ports = len(near_ports)
        nearby_bikes = sum(b["bikes"] for _, b in near_ports)
        top_raw_ports = collect_top_available_ports(near_ports, limit=3)

        if nearby_bikes >= 1:
            category = "avoidable"
            category_label = "回避可能"
            avoidable_count += 1
        elif nearby_ports >= 1:
            category = "out_of_stock"
            category_label = "空き0台（貸出不可）・再配置不足"
            out_of_stock_count += 1
        else:
            category = "no_port"
            category_label = "ポート空白・インフラ不足"
            no_port_count += 1

        # 下流バス停への「バス待ち vs シェアサイクル切替」比較計算（上位3ポートそれぞれで算出）
        comparison_trip = None
        selected_dst = []
        best_trip = item["best_trip"]

        if category == "avoidable" and best_trip and top_raw_ports:
            selected_dst = select_representative_downstream_stops(
                best_trip["downstream_candidates"]
            )
            comparison_trip = {
                "route_name": best_trip["route_name"],
                "headsign": best_trip["headsign"],
                "delay_min": best_trip["delay_min"],
            }

        available_ports_300m = []
        for raw_p in top_raw_ports:
            port_comparisons = (
                build_comparisons_for_origin_port(raw_p, selected_dst)
                if selected_dst and category == "avoidable"
                else []
            )
            clean_p = {k: v for k, v in raw_p.items() if k != "raw_distance_m"}
            clean_p["downstream_comparisons"] = port_comparisons
            available_ports_300m.append(clean_p)

        nearest_port = available_ports_300m[0] if available_ports_300m else None
        downstream_comparisons = (
            nearest_port["downstream_comparisons"] if nearest_port else []
        )

        headsigns = sorted(item["departing_headsigns"] or item["all_headsigns"])
        sorted_arrivals = sorted(
            item["arrivals"],
            key=lambda a: (a["_predicted_ts"], a["route_name"]),
        )
        arrivals = [
            {k: v for k, v in a.items() if k != "_predicted_ts"}
            for a in sorted_arrivals
        ]
        next_arrival = arrivals[0] if arrivals else None

        delayed_stops.append(
            {
                "stop_id": sid,
                "name": item["name"],
                "lat": item["lat"],
                "lon": item["lon"],
                "max_delay_min": round(item["max_delay_sec"] / 60, 1),
                "routes": sorted(item["routes"]),
                "headsigns": headsigns,
                "delayed_trips": item["delayed_trips"],
                "next_arrival": next_arrival,
                "arrivals": arrivals,
                "nearby_ports_300m": nearby_ports,
                "nearby_bikes_300m": nearby_bikes,
                "category": category,
                "category_label": category_label,
                "avoidable": category == "avoidable",
                "nearest_port": nearest_port,
                "available_ports_300m": available_ports_300m,
                "comparison_trip": comparison_trip,
                "downstream_comparisons": downstream_comparisons,
            }
        )

    delayed_stops.sort(key=lambda x: x["max_delay_min"], reverse=True)
    feed_time_str = datetime.fromtimestamp(feed_ts, JST).strftime("%Y-%m-%d %H:%M:%S JST")

    return {
        "source": source,
        "fallback_reason": fallback_reason,
        "realtime_configured": realtime_configured,
        "feed_time": feed_time_str,
        "delayed_stop_count": len(delayed_stops),
        "avoidable_stop_count": avoidable_count,
        "out_of_stock_stop_count": out_of_stock_count,
        "no_port_stop_count": no_port_count,
        "bikeshare_port_count": len(bikes),
        "delayed_stops": delayed_stops,
        "bikeshare_ports": bikes,
    }


@app.get("/api/stop-detail")
def get_stop_detail(stop_id: str = Query(..., min_length=1)):
    """遅延リストにないマイバス停向けに、全バス停マスタと半径300m内ポート案内を返す。"""
    if not STOPS:
        load_static_gtfs()

    sid = stop_id.strip()
    if sid not in STOPS:
        raise HTTPException(status_code=404, detail="Stop not found")

    st_info = STOPS[sid]
    bikes = fetch_bikeshare_stations(force_refresh=False)
    near_ports = find_ports_near_coords(st_info["lat"], st_info["lon"], bikes)
    nearby_ports = len(near_ports)
    nearby_bikes = sum(b["bikes"] for _, b in near_ports)
    top_raw_ports = collect_top_available_ports(near_ports, limit=3)

    available_ports_300m = [
        {**{k: v for k, v in p.items() if k != "raw_distance_m"}, "downstream_comparisons": []}
        for p in top_raw_ports
    ]
    nearest_port = available_ports_300m[0] if available_ports_300m else None

    return {
        "stop_id": sid,
        "name": st_info["name"],
        "lat": st_info["lat"],
        "lon": st_info["lon"],
        "has_delay": False,
        "category": "normal",
        "category_label": "平常運行",
        "max_delay_min": 0,
        "routes": [],
        "headsigns": [],
        "delayed_trips": 0,
        "next_arrival": None,
        "arrivals": [],
        "nearby_ports_300m": nearby_ports,
        "nearby_bikes_300m": nearby_bikes,
        "nearest_port": nearest_port,
        "available_ports_300m": available_ports_300m,
        "comparison_trip": None,
        "downstream_comparisons": [],
    }


GITHUB_DISPATCH_URL = (
    "https://api.github.com/repos/atsushiyago/odpt-bikeshare-collector"
    "/actions/workflows/collect.yml/dispatches"
)


@app.post("/trigger-collect")
def trigger_collect():
    """Cloud Scheduler から GitHub Actions の収集ワークフローを起動する中継エンドポイント。"""
    github_pat = (os.environ.get("GITHUB_PAT") or "").strip()
    if not github_pat:
        raise HTTPException(status_code=500, detail="GITHUB_PAT is not configured")

    headers = {
        "Authorization": f"Bearer {github_pat}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "odpt-yokohama-bus-scheduler",
    }

    try:
        res = requests.post(
            GITHUB_DISPATCH_URL,
            headers=headers,
            json={"ref": "main"},
            timeout=15,
        )
    except requests.RequestException as exc:
        logger.error("GitHub Actions dispatch request failed: %s", exc)
        raise HTTPException(
            status_code=502,
            detail="Failed to reach GitHub API",
        ) from exc

    if 200 <= res.status_code < 300:
        return {"status": "dispatched"}

    logger.error(
        "GitHub Actions dispatch failed: status=%s body=%s",
        res.status_code,
        res.text,
    )
    raise HTTPException(
        status_code=502,
        detail=f"GitHub API returned status {res.status_code}",
    )


LOCAL_ANALYSIS_PATH = (
    BASE_DIR.parent / "odpt-bikeshare-collector" / "data" / "analysis_summary.json"
)
LOCAL_ANALYSIS_CACHE = {"mtime": 0.0, "data": None}
ANALYSIS_SUMMARY_URL = (
    "https://raw.githubusercontent.com/atsushiyago/odpt-bikeshare-collector"
    "/main/data/analysis_summary.json"
)
ANALYSIS_CACHE_TTL_SEC = 300.0
ANALYSIS_CACHE = {"data": None, "fetched_at": 0.0}
ANALYSIS_CACHE_LOCK = threading.Lock()


@app.get("/api/analysis")
def get_analysis_summary():
    """ローカル集計 JSON（存在時）または GitHub 上の蓄積データ分析サマリー JSON を取得する。"""
    if LOCAL_ANALYSIS_PATH.exists():
        try:
            mtime = LOCAL_ANALYSIS_PATH.stat().st_mtime
            with ANALYSIS_CACHE_LOCK:
                if LOCAL_ANALYSIS_CACHE["data"] is not None and LOCAL_ANALYSIS_CACHE["mtime"] == mtime:
                    return LOCAL_ANALYSIS_CACHE["data"]
            payload = json.loads(LOCAL_ANALYSIS_PATH.read_text(encoding="utf-8"))
            with ANALYSIS_CACHE_LOCK:
                LOCAL_ANALYSIS_CACHE["mtime"] = mtime
                LOCAL_ANALYSIS_CACHE["data"] = payload
            return payload
        except (OSError, ValueError) as exc:
            logger.warning("Failed to read local analysis summary: %s", exc)

    now = time.monotonic()
    with ANALYSIS_CACHE_LOCK:
        cached_data = ANALYSIS_CACHE["data"]
        cached_at = ANALYSIS_CACHE["fetched_at"]
        if cached_data is not None and (now - cached_at) < ANALYSIS_CACHE_TTL_SEC:
            return cached_data

    try:
        res = requests.get(ANALYSIS_SUMMARY_URL, timeout=15)
        res.raise_for_status()
        payload = res.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("Failed to fetch analysis summary: %s", exc)
        with ANALYSIS_CACHE_LOCK:
            if ANALYSIS_CACHE["data"] is not None:
                return ANALYSIS_CACHE["data"]
        raise HTTPException(
            status_code=502,
            detail="Failed to fetch analysis summary from GitHub",
        ) from exc

    with ANALYSIS_CACHE_LOCK:
        ANALYSIS_CACHE["data"] = payload
        ANALYSIS_CACHE["fetched_at"] = time.monotonic()
    return payload
