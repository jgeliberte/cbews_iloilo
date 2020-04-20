from flask import Blueprint, jsonify, request, redirect, url_for, send_from_directory, send_file
from connections import SOCKETIO
from pprint import pprint
import time
import os
from src.model.community_risk_assessment import CommunityRiskAssessment
from src.api.helpers import Helpers as h
from config import APP_CONFIG

HAZARD_MAPS_BLUEPRINT = Blueprint("hazard_maps_blueprint", __name__)

@HAZARD_MAPS_BLUEPRINT.route("/cra/hazard_maps/fetch/<site_id>", methods=["GET"])
def fetch(site_id):
    site_dir = APP_CONFIG['site_code'][site_id]
    file_loc = APP_CONFIG[site_dir]

    basepath = f'{file_loc}/MAPS'
    map_list = os.listdir(basepath)

    maps = []

    for map in map_list:
        path = os.path.join(basepath, map)
        if not os.path.isdir(path):
            file_type = map.split(".")[1]
            maps.append({
                "filename": map,
                "file_type": file_type,
                "file_path": basepath
            })

    return jsonify({"status": True, "data": maps})


@HAZARD_MAPS_BLUEPRINT.route("/cra/hazard_maps/upload", methods=["POST"])
def upload():
    try:
        file = request.files['file']
        directory = f"{APP_CONFIG['MARIRONG_DIR']}/MAPS/"
        filename = file.filename

        count = filename.count(".")
        name_list = filename.split(".", count)
        file_type = f".{name_list[count]}"
        name_list.pop()
        filename = f"{'.'.join(name_list)}"

        temp = f"{filename}{file_type}"
        uniq = 1
        while os.path.exists(f"{directory}{temp}"):
            temp = '%s_%d%s' % (filename, uniq, file_type)
            uniq += 1

        file.save(os.path.join(directory, temp))

        return_data = { "status": True }
    except Exception as err:
        # raise err
        return_data = { "status": False }

    return jsonify(return_data)
