from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
import json
from datetime import datetime as dt
from datetime import timedelta as td
from src.model.eq_data import Earthquake as eq

EARTHQUAKE_BLUEPRINT = Blueprint("earthquake_blueprint", __name__)

@EARTHQUAKE_BLUEPRINT.route("/sensor_data/earthquake/", methods=["GET"])
def fetch():
    eq_data = eq.get_latest_eq_events()
    reconstruct_data = []
    for x in eq_data:
        reconstruct_data.append({
            'eq_id': x[0],
            'ts': str(x[1]),
            'mag': x[2],
            'dept': x[3],
            'lat': x[4],
            'lon': x[5],
            'critical_distance': x[6],
            'issuer': x[7],
            'processed': x[8]
        })
    if len(reconstruct_data) != 0:
        ret_val = {
            'status': True,
            'data': reconstruct_data
        }
    else:
        ret_val = {
            'status': False,
            'message': 'Error fetching earthquake data'
        }
    return jsonify(ret_val)