from flask import Blueprint, jsonify, request, redirect, url_for, send_from_directory, send_file
from connections import SOCKETIO
from pprint import pprint
import time
import os
from src.model.community_risk_assessment import CommunityRiskAssessment
from config import APP_CONFIG

COMMUNITY_RISK_ASSESSMENT_BLUEPRINT = Blueprint("community_risk_assessment", __name__)

@COMMUNITY_RISK_ASSESSMENT_BLUEPRINT.route("/cra/community_risk_assessment/fetch", methods=["POST"])
def fetch():
    data = request.get_json()
    cra_list = os.listdir(data['path'])

    files = []

    for file in cra_list:
        file_type = file.split(".")[1]
        files.append({
            "filename": file,
            "file_type": file_type,
            "file_path": data['path']
        })
    # return {"status": True, "data": files}
    return jsonify({"status": True, "data": files})


@COMMUNITY_RISK_ASSESSMENT_BLUEPRINT.route("/cra/community_risk_assessment/upload", methods=["POST"])
def upload():
    try:
        file = request.files['file']
        directory = f"{APP_CONFIG['MARIRONG_DIR']}/DOCUMENTS/"
        filename = file.filename

        # if os.path.exists(f"{directory}{filename}"):
        #     count = filename.count(".")
        #     name_list = filename.split(".", count)
        #     file_type = f".{name_list[count]}"
        #     name_list.pop()
        #     filename = f"{'.'.join(name_list)}-copy{file_type}"
        # file.save(os.path.join(directory, filename))


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
        raise err
        # return_data = { "status": False }

    return jsonify(return_data)


# @COMMUNITY_RISK_ASSESSMENT_BLUEPRINT.route("/cra/community_risk_assessment/download", methods=["POST"])
# # @COMMUNITY_RISK_ASSESSMENT_BLUEPRINT.route("/cra/community_risk_assessment/download/<filename>", methods=["POST"])
# def download():
#     print("==================================")
#     data = request.get_json()
#     print(data)
#     filename = data["filename"]
#     print("FILENAME", filename)
#     file_type = filename.split(".")[1]
#     mime_type = f"application/{file_type}"
#     print("======")
#     print(file_type)
#     try:
#         return send_from_directory(data["file_path"], filename, as_attachment=True, mimetype=mime_type, attachment_filename=(str(filename) + f".{file_type}"))
        
#     except Exception as e:
#         print(e)
#         return jsonify({ "status": False})
