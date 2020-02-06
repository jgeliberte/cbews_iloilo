from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
from datetime import datetime as dt
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


@SURFICIAL_MARKERS_BLUEPRINT.route("/ground_data/surficial_markers/modify", methods=["PATCH"])
def modify():
    try:
        data = request.get_json()
        current_ts = str(dt.today())
        data['new_ts'] = str(dt.strptime(data['new_ts'], '%Y-%m-%d %H:%M:%S'))
        data['ref_ts'] = str(dt.strptime(data['ref_ts'], '%Y-%m-%d %H:%M:%S'))

        if data['new_ts'] > current_ts:
            surficial = {
                "status": False,
                "message": "Failed to modify surficial data. Data timestamp out of bounce."
            }
        else:
            mo_ret_val = GroundData.fetch_surficial_mo_id(data['ref_ts'], data['site_id'])
            status = GroundData.update_surficial_marker_values(mo_ret_val[0][0], data)

        surficial = {"status": True}
    except Exception as err:
        surficial = {
            "status": False,
            "message": f"Failed to modify surficial data. Error: {err}"
        }
    return jsonify(surficial)