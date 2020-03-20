from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
import json
import time
from datetime import datetime as dt
from datetime import timedelta as td
from src.model.ground_data import GroundData

SURFICIAL_ANALYSIS_BLUEPRINT = Blueprint("surficial_analysis_blueprint", __name__)

@SURFICIAL_ANALYSIS_BLUEPRINT.route("/data_analysis/surficial/plot/data/<site_code>", methods=["GET"])
def fetch(site_code):
    plot_data = []
    surficial_plot = []
    markers = GroundData.fetch_surficial_markers(29) #Leave this for now
    ts_end = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    ts_start = dt.today() - td(days=200) # for data sampling
    ts_start = ts_start.strftime("%Y-%m-%d %H:%M:%S")
    for marker in markers:
        prelim_data = {
            'marker_id': marker[0],
            'marker_name': marker[1],
            'name': marker[1],
            'data': []
        }
        surficial_plot_data = GroundData.fetch_surficial_plot_data(marker[0], site_code, ts_start, ts_end)
        for row in surficial_plot_data:
            prelim_data['data'].append({
                'x': row[3].timestamp(),
                'y': row[4],
                'data_id': row[1],
                'mo_id': row[0], 
            })
        surficial_plot.append(prelim_data)
    ret_val = {
        'status': True, 
        'surficial_plot': surficial_plot,
        'ts_start': ts_start,
        'ts_end': ts_end,
        'site_code': site_code
    }
    return jsonify(ret_val)