from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
import json
from datetime import datetime as dt
from datetime import timedelta as td
import analysis_pycodes.analysis.rainfall.rainfall as rainfall_analysis
# from src.model.ground_data import GroundData

RAINFALL_ANALYSIS_BLUEPRINT = Blueprint("rainfall_analysis_blueprint", __name__)

@RAINFALL_ANALYSIS_BLUEPRINT.route("data_analysis/rainfall/plot/data/<site_code>", methods=["GET"])
def fetch(site_code):
    data_ts_end = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    data_ts_start = dt.today() - td(days=7)
    plot_data = rainfall_analysis.main(site_code = site_code, end=data_ts_end, days=7)
    rain_data = json.loads(plot_data)
    rain_data[0]['ts_end'] = data_ts_end
    rain_data[0]['ts_start'] = data_ts_start.strftime("%Y-%m-%d %H:%M:%S")
    return jsonify(rain_data)