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
        print(request.get_json())
        (ts, feature_id, feature_name, reporter, location, description, site_id, user_id) = request.get_json().values()
        instance = GroundData.fetch_feature_name(feature_id, feature_name, site_id)
        if len(instance) == 0:
            instance = GroundData.insert_moms_instance(site_id, feature_id, feature_name, 
                                                            location, reporter)
        else:
            instance = instance[0][0]
        moms_id = GroundData.insert_moms_record(instance, ts, user_id, description)
        if moms_id['status'] == True:
            moms = {
                "status": False,
                "message": "Successfully added new Manifestation of Movements data.",
                "moms_id": moms_id['data']
            }
        else:
            moms = {
                "status": False,
                "message": f"Failed to add moms data."
            }
    except Exception as err:
        moms = {
            "status": False,
            "message": f"Failed to fetch moms data. Error: {err}"
        }
    return jsonify(moms)

@MANIFESTATION_OF_MOVEMENTS_BLUEPRINT.route("/ground_data/moms/fetch/feature/<feature_id>/<site_id>", methods=["GET"])
def fetch_feature(feature_id, site_id):
    try:
        name_container = []
        feature_names = GroundData.fetch_feature_name(feature_id, None, site_id)
        for name in feature_names:
            (instance_id, feature_name, location, reporter) = name
            name_container.append({
                "instance_id":instance_id,
                "feature_name":feature_name,
                "location": location,
                "reporter": reporter
            })
        moms = {"status": True, "data": name_container}
    except Exception as err:
        moms = {
            "status": False,
            "message": f"Failed to fetch moms data. Error: {err}"
        }
    return jsonify(moms)