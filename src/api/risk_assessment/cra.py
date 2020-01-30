from flask import Blueprint, jsonify, request, redirect, url_for, send_from_directory
from connections import SOCKETIO
from pprint import pprint
import time
import os
from src.model.community_risk_assessment import CommunityRiskAssessment

COMMUNITY_RISK_ASSESSMENT_BLUEPRINT = Blueprint("community_risk_assessment", __name__)

@COMMUNITY_RISK_ASSESSMENT_BLUEPRINT.route("/cra/community_risk_assessment/fetch", methods=["POST"])
def fetch():
    data = request.get_json()
    cra_list = os.listdir(data['path'])

    files = []

    for file in cra_list:
        files.append({
            "filename": file,
            "file_type": file.split(".")[1],
            "file_path": data['path']
        })
    return jsonify({"status": True, "data": files})

@COMMUNITY_RISK_ASSESSMENT_BLUEPRINT.route("/cra/community_risk_assessment/upload", methods=["POST"])
def upload():
    file = request.files['resource']
    file.save(os.path.join('/var/www/html/CBEWSL/MARIRONG/DOCUMENTS/', file.filename))
    return jsonify({"status": True})