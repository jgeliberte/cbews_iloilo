from flask import Blueprint, jsonify, request
from flask_cors import CORS, cross_origin
from connections import SOCKETIO
from src.model.maintenance import Maintenance as m
from src.api.helpers import Helpers as h

MAINTENANCE_LOGS_BLUEPRINT = Blueprint("maintenance_logs_blueprint", __name__)

@MAINTENANCE_LOGS_BLUEPRINT.route("/maintenance/maintenance_logs/add", methods=["POST"])
@cross_origin()
def add():
    data = request.get_json()
    status = m.create_maintenance_log(m, data)
    if status is not None:
        return_value = {
            "status": True,
            "maintenance_log_id": status,
            "message": "New maintenance log data successfully added!"
        }
    else:
        return_value = {
            "status": False,
            "maintenance_log_id": None,
            "message": "Failed to add maintenance log data. Please check your network connection."
        }
    return jsonify(return_value)

@MAINTENANCE_LOGS_BLUEPRINT.route("/maintenance/maintenance_logs/fetch", methods=["POST"])
@MAINTENANCE_LOGS_BLUEPRINT.route("/maintenance/maintenance_logs/fetch/<site_id>/<maintenance_log_id>", methods=["GET"])
@cross_origin()
def fetch(site_id=None, maintenance_log_id=None):
    """Returns maintenance log in two forms of filter. One is via get request
    """
    try:
        json_data = request.get_json()
        ts_dict = None
        if json_data:
            ts_dict = json_data["ts_dict"]
            site_id = json_data["site_id"]

        result = m.fetch_maintenance_log(m,
                    site_id=site_id, maintenance_log_id=maintenance_log_id, ts_dict=ts_dict)
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

@MAINTENANCE_LOGS_BLUEPRINT.route("/maintenance/maintenance_logs/update", methods=["POST"])
def modify():
    data = request.get_json()
    result = m.update_maintenance_log(m, data={
        "maintenance_log_id": int(data['maintenance_log_id']),
        "maintenance_ts": data['maintenance_ts'],
        "maintenance_type": data['maintenance_type'],
        "remarks": data['remarks'],
        "in_charge": data['in_charge'],
        "updater": data['updater'],
        "site_id": int(data['site_id'])
    })
    if result is not None:
        return_value = {
            "status": True,
            "message": "Maintenance logs data successfully updated!"
        }
    else:
        return_value = {
            "status": False,
            "message": "Failed to update maintenance logs data. Please check your network connection."
        }
    return jsonify(return_value)

@MAINTENANCE_LOGS_BLUEPRINT.route("/maintenance/maintenance_logs/remove/<site_id>/<maintenance_log_id>", methods=["DELETE"])
def remove(site_id, maintenance_log_id):
    status = m.delete_maintenance_log(m, maintenance_log_id, site_id)
    if status is not None:
        return_value = {
            "status": True,
            "message": "Maintenance log data successfully deleted!"
        }
    else:
        return_value = {
            "status": False,
            "message": "Failed to delete maintenance log data. Please check your network connection."
        }
    return jsonify(return_value)
