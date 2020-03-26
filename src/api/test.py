from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys, json
from datetime import datetime as dt, timedelta
from src.model.alert_generation import AlertGeneration
from src.model.public_alert_table import PublicAlertTable as PAT
from src.model.users import Users
from src.api.helpers import Helpers as h
from src.api.alert_generation import candidate_alerts_generator


TEST_BLUEPRINT = Blueprint("test_blueprint", __name__)


@TEST_BLUEPRINT.route("/test_fetch_alert_status", methods=["GET"])
def test_fetch_alert_status():
    """
    """
    ag = AlertGeneration

    result = ag.fetch_alert_status(ag, trigger_id=25760)

    h.var_checker("result", result, True)
    
    return jsonify(result)


@TEST_BLUEPRINT.route("/update_alert_status", methods=["GET"])
def test_update_alert_status():
    """
    """
    ag = AlertGeneration
    now_ts_str = h.dt_to_str(dt.now())

    result = ag.update_alert_status(ag,
        update_dict={
            "alert_status": 0,
            "remarks": "Validating mo muna",
            "ts_set": now_ts_str,
            "ts_ack": now_ts_str
        },
        where_dict={
            "trigger_id": 122213
        }
    )

    h.var_checker("result", result, True)

    return jsonify(result)


@TEST_BLUEPRINT.route("/insert_alert_status", methods=["GET"])
def test_insert_alert_status():
    """
    """
    ag = AlertGeneration
    now_ts_str = h.dt_to_str(dt.now())

    result = ag.insert_alert_status(ag,
        trigger_id=122213,
        ts_last_retrigger="2020-03-25 13:30:00",
        ts_set=now_ts_str,
        ts_ack=now_ts_str,
        alert_status=0,
        remarks="Test remarks",
        user_id=1
    )

    h.var_checker("result", result, True)

    return jsonify(result)


@TEST_BLUEPRINT.route("/update_public_alert_event", methods=["GET"])
def test_update_public_alert_event():
    """
    """
    ag = AlertGeneration
    now_ts_str = h.dt_to_str(dt.now())

    result = PAT.update_public_alert_event(PAT,
        update_dict={
            "alert_status": 0,
            "remarks": "Validating mo muna",
            "ts_set": now_ts_str,
            "ts_ack": now_ts_str
        },
        where_dict={
            "trigger_id": 122213
        }
    )
    h.var_checker("result", result, True)

    return jsonify(result)


