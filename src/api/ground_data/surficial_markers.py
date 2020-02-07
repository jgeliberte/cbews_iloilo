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
        marker_values_update = True
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
            marker_ids = dict(map(reversed, GroundData.fetch_marker_ids(mo_ret_val[0][0])))
            for x in data['marker_values']:
                status = GroundData.update_surficial_marker_values(mo_ret_val[0][0], marker_ids[x], data['marker_values'][x])
                if status == None:
                    marker_values_update = False
            if marker_values_update == True:
                status = GroundData.update_surficial_marker_observation(mo_ret_val[0][0], 
                        data['new_ts'], data['weather'], data['observer'], data['site_id'])
                if status != None:
                    surficial = {
                        "status": True,
                        "message": "Successfull modification for surficial data."
                    }
            else:
                surficial = {
                    "status": False,
                    "message": f"Failed to modify surficial data. Error: updating marker values"
                }
    except Exception as err:
        surficial = {
            "status": False,
            "message": f"Failed to modify surficial data. Error: {err}"
        }
    return jsonify(surficial)