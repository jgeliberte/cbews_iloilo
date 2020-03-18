from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
import json
from datetime import datetime as dt
from datetime import timedelta as td
from src.model.ground_data import GroundData

SURFICIAL_ANALYSIS_BLUEPRINT = Blueprint("surficial_analysis_blueprint", __name__)

@SURFICIAL_ANALYSIS_BLUEPRINT.route("data_analysis/surficial/plot/data/<site_code>", methods=["GET"])
def fetch(site_code):
    plot_data = []
    markers = GroundData.fetch_surficial_markers(29) #Leave this for now
    ts_end = dt.today().strftime("%Y-%m-%d %H:%M:%S")
    ts_start = dt.today() - td(days=7)
    ts_start = ts_start.strftime("%Y-%m-%d %H:%M:%S")
    for marker in markers:
        surficial_plot_data = GroundData.fetch_surficial_plot_data(marker[1], site_code, ts_start, ts_end)
        print(surficial_plot_data)
        
    return jsonify({'status': True})