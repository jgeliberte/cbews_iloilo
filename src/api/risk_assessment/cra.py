from flask import Blueprint, jsonify, request, redirect, url_for, send_from_directory, send_file
from connections import SOCKETIO
from pprint import pprint
import time
import os
from src.model.community_risk_assessment import CommunityRiskAssessment
from src.api.helpers import Helpers as h
from config import APP_CONFIG

COMMUNITY_RISK_ASSESSMENT_BLUEPRINT = Blueprint("community_risk_assessment", __name__)

@COMMUNITY_RISK_ASSESSMENT_BLUEPRINT.route("/cra/community_risk_assessment/fetch", methods=["POST"])
def fetch():
    data = request.get_json()
    basepath = data['path']
    cra_list = os.listdir(basepath)

    files = []

    for file in cra_list:
        path = os.path.join(basepath, file)
        if not os.path.isdir(path):
            file_type = file.split(".")[1]
            files.append({
                "filename": file,
                "file_type": file_type,
                "file_path": basepath
            })
    # return {"status": True, "data": files}
    return jsonify({"status": True, "data": files})


@COMMUNITY_RISK_ASSESSMENT_BLUEPRINT.route("/cra/community_risk_assessment/upload", methods=["POST"])
def upload():
    try:
        file = request.files['file']
        directory = f"{APP_CONFIG['MARIRONG_DIR']}/DOCUMENTS/"
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
