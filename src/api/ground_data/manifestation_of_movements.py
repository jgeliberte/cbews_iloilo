from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
from datetime import datetime as dt
from src.model.ground_data import GroundData

MANIFESTATION_OF_MOVEMENTS_BLUEPRINT = Blueprint("manifestation_of_movements_blueprint", __name__)

@MANIFESTATION_OF_MOVEMENTS_BLUEPRINT.route("/ground_data/moms/fetch/<site_id>", methods=["GET"])
def fetch(site_id):
    try:
        print(site_id)
        moms = {
            "status": True,
            "message": f"Success"
        }
    except Exception as err:
        moms = {
            "status": False,
            "message": f"Failed to fetch moms data. Error: {err}"
        }
    return jsonify(moms)