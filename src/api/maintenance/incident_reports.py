from flask import Blueprint, jsonify, request
from flask_cors import CORS, cross_origin
from connections import SOCKETIO
from src.model.maintenance import Maintenance as m
from src.api.helpers import Helpers as h
from config import APP_CONFIG

INCIDENT_REPORTS_BLUEPRINT = Blueprint("incident_reports_blueprint", __name__)


@INCIDENT_REPORTS_BLUEPRINT.route("/maintenance/incident_reports/add", methods=["POST"])
@cross_origin()
def add():
    data = request.get_json()
    status = m.create_incident_report(m, data)
    if status is not None:
        return_value = {
            "ok": True,
            "ir_id": status,
            "message": "New incident report data successfully added!"
        }
    else:
        return_value = {
            "ok": False,
            "ir_id": None,
            "message": "Failed to add incident report data. Please check your network connection."
        }
    return jsonify(return_value)


@INCIDENT_REPORTS_BLUEPRINT.route("/maintenance/incident_reports/fetch", methods=["POST"])
@INCIDENT_REPORTS_BLUEPRINT.route("/maintenance/incident_reports/fetch/<site_id>/<ir_id>", methods=["GET"])
@cross_origin()
def fetch(site_id=None, ir_id=None):
    """Returns incident report in two forms of filter. One is via get request
    """
    try:
        json_data = request.get_json()
        ts_dict = None
        if json_data:
            ts_dict = json_data["ts_dict"]
            site_id = json_data["site_id"]

        result = m.fetch_incident_report(m,
                                         site_id=site_id, ir_id=ir_id, ts_dict=ts_dict)
        response = {
            "ok": True,
            "data": result
        }

    except Exception as err:
        print(err)
        response = {
            "ok": False,
            "data": []
        }

    return jsonify(response)


@INCIDENT_REPORTS_BLUEPRINT.route("/maintenance/incident_reports/update", methods=["POST"])
@cross_origin()
def modify():
    data = request.get_json()
    result = m.update_incident_report(m, data={
        "ir_id": int(data['ir_id']),
        "report_ts": data['report_ts'],
        "description": data['description'],
        "reporter": data['reporter'],
        "site_id": int(data['site_id'])
    })
    if result is not None:
        return_value = {
            "ok": True,
            "message": "Incident report data successfully updated!"
        }
    else:
        return_value = {
            "ok": False,
            "message": "Failed to update incident reports data. Please check your network connection."
        }
    return jsonify(return_value)


@INCIDENT_REPORTS_BLUEPRINT.route("/maintenance/incident_reports/remove/<site_id>/<ir_id>", methods=["GET"])
@cross_origin()
def remove(site_id, ir_id):
    status = m.delete_incident_report(m, ir_id, site_id)
    if status is not None:
        return_value = {
            "ok": True,
            "message": "Maintenance log data successfully deleted!"
        }
    else:
        return_value = {
            "ok": False,
            "message": "Failed to delete incident report data. Please check your network connection."
        }
    return jsonify(return_value)


@INCIDENT_REPORTS_BLUEPRINT.route("/maintenance/incident_reports/upload_report_attachment", methods=["POST"])
@cross_origin()
def upload_report_attachment():
    try:
        file = request.files['file']

        h.var_checker("request.form", request.form, True)
        form_json = request.form.to_dict(flat=False)
        ir_id = form_json["ir_id"][0]
        file_path = f"{APP_CONFIG['MARIRONG_DIR']}/DOCUMENTS/INCIDENT_REPORTS/{ir_id}/"
        final_path = h.upload(file=file, file_path=file_path)

        response = {
            "ok": True,
            "message": "Report attachment OKS!",
            "file_path": final_path
        }

    except Exception as err:
        print(err)
        response = {
            "ok": False,
            "message": "Report attachment NOT oks!",
            "file_path": "ERROR"
        }

    return jsonify(response)


@INCIDENT_REPORTS_BLUEPRINT.route("/maintenance/incident_reports/fetch_report_attachments/<ir_id>", methods=["GET"])
@cross_origin()
def fetch_report_attachments(ir_id):
    try:

        file_path = f"{APP_CONFIG['MARIRONG_DIR']}/DOCUMENTS/INCIDENT_REPORTS/{ir_id}/"
        files = h.fetch(file_path)

        response = {
            "ok": True,
            "message": "Report attachment fetch OKS!",
            "data": files
        }

    except Exception as err:
        print(err)
        response = {
            "ok": False,
            "message": "Report attachment fetch NOT oks!",
            "data": files
        }

    return jsonify(response)
