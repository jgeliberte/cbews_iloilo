from flask import Blueprint, jsonify, request
from connections import SOCKETIO
from src.model.community_risk_assessment import CommunityRiskAssessment

CAPACITY_AND_VULNERABILITY_BLUEPRINT = Blueprint("capacity_and_vulnerability_blueprint", __name__)

@CAPACITY_AND_VULNERABILITY_BLUEPRINT.route("/cra/capacity_and_vulnerability/add", methods=["POST"])
def add():
    data = request.get_json()
    status = CommunityRiskAssessment.create_cav(data)
    if status is not None:
        return_value = {
            "status": True,
            "cav_id": status,
            "message": "New capacity and vulnerability data successfully added!"
        }
    else:
        return_value = {
            "status": False,
            "cav_id": None,
            "message": "Failed to add capacity and vulnerability data. Please check your network connection."
        }
    return jsonify(return_value)