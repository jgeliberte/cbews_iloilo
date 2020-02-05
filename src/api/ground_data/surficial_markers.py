from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
from src.model.ground_data import GroundData

SURFICIAL_MARKERS_BLUEPRINT = Blueprint("surficial_markers_blueprint", __name__)

@SURFICIAL_MARKERS_BLUEPRINT.route("/ground_data/surficial_markers/fetch/<site_id>", methods=["GET"])
def fetch(site_id):
    try:
        temp_ts = ""
        temp_item = {}
        temp_dt_row = []
        surficial_data = GroundData.fetch_surficial_data(site_id)
        surficial_markers = GroundData.fetch_surficial_markers(site_id)
        for row in surficial_data:
            (ts, measurement, marker, observer, weather) = row
            ts = str(ts)
            if ts in temp_item:
                if not marker in temp_item[ts]:
                    temp_item[ts][marker] = measurement
            else:
                if temp_item:
                    temp_dt_row.append(temp_item)

                temp_dt_item = {
                    "ts": ts,
                    "weather": weather,
                    "observer": observer,
                    marker: measurement
                }
                temp_item = {
                    ts: temp_dt_item
                }
        surficial = {
            "data": temp_dt_row,
            "markers": surficial_markers,
            "status": True,
            "message": "Fetch success!"
        }
    except Exception as err:
        surficial = {
            "status": False,
            "message": f"Failed to fetch surficial data. Error: {err}"
        }
    return jsonify(surficial)