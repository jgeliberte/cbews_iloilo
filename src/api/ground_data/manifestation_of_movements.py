from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
from datetime import datetime as dt
from src.model.ground_data import GroundData

MANIFESTATION_OF_MOVEMENTS_BLUEPRINT = Blueprint("manifestation_of_movements_blueprint", __name__)

@MANIFESTATION_OF_MOVEMENTS_BLUEPRINT.route("/ground_data/moms/fetch/<site_id>", methods=["GET"])
def fetch(site_id):
    try:
        moms_list = GroundData.fetch_moms(site_id)
        moms = {
            "status": True,
            "data": moms_list
        }
    except Exception as err:
        moms = {
            "status": False,
            "message": f"Failed to fetch moms data. Error: {err}"
        }
    return jsonify(moms)

@MANIFESTATION_OF_MOVEMENTS_BLUEPRINT.route("/ground_data/moms/add", methods=["POST"])
def add():
    try:
        (ts, feature_id, feature_name, reporter, description, site_id) = request.get_json().values()
        moms_instance = GroundData.insert_moms_instance(site_id, feature_id, feature_name, 
                                                        description, reporter)
        print(moms_instance)
    except Exception as err:
        moms = {
            "status": False,
            "message": f"Failed to fetch moms data. Error: {err}"
        }
    return jsonify({"status": True})

@MANIFESTATION_OF_MOVEMENTS_BLUEPRINT.route("/ground_data/moms/fetch/feature/<feature_id>/<site_id>", methods=["GET"])
def fetch_feature(feature_id, site_id):
    try:
        name_container = []
        feature_names = GroundData.fetch_feature_name(feature_id, site_id)
        for name in feature_names:
            (instance_id, feature_name) = name
            name_container.append({
                "instance_id":instance_id,
                "feature_name":feature_name
            })
        moms = {"status": True, "data": name_container}
    except Exception as err:
        moms = {
            "status": False,
            "message": f"Failed to fetch moms data. Error: {err}"
        }
    return jsonify(moms)