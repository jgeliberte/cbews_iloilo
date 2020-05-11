from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
from datetime import datetime as dt
from src.model.ground_data import GroundData
from src.model.alert_generation import AlertGeneration as AlertGen
from src.api.helpers import Helpers as H

ON_DEMAND_BLUEPRINT = Blueprint("on_demand_blueprint", __name__)

@ON_DEMAND_BLUEPRINT.route("/ground_data/on_demand/fetch/<site_id>", methods=["GET"])
def fetch(site_id):
    try:
        od_list = GroundData.get_latest_od_events(site_id=site_id)
        od = {
            "ok": True,
            "data": od_list
        }
    except Exception as err:
        od = {
            "ok": False,
            "message": f"Failed to fetch od data. Error: {err}"
        }
    return jsonify(od)


@ON_DEMAND_BLUEPRINT.route("/ground_data/on_demand/add", methods=["POST"])
def add():
    try:
        print(request.get_json())
        (alert_level, reason, reporter, site_id, ts) = request.get_json().values()

        # trigger_sym_id = AlertGen.get_operational_trigger_symbol(
        #                             trigger_source='on demand',
        #                             alert_level=1,
        #                             return_col="trigger_sym_id")

        # op_trig_data_dict = AlertGen.fetch_recent_operational_trigger(
        #     AlertGen,
        #     site_id=site_id,
        #     trig_sym_id=trigger_sym_id
        # )
        # H.var_checker("op_trig_data_dict", op_trig_data_dict, True)

        # # If nothing exists in database:
        # if not op_trig_data_dict:
        #     trigger_id = AlertGen.insert_operational_trigger(
        #         site_id=site_id,
        #         trig_sym_id=trigger_sym_id,
        #         ts_updated=ts
        #     )
        # # Else update especially ts in database:
        # else:
        #     trigger_id = op_trig_data_dict["trigger_id"]
        #     result = AlertGen.update_operational_trigger(
        #         op_trig_id=trigger_id,
        #         trig_sym_id=trigger_sym_id,
        #         ts_updated=ts
        #     )

        result = GroundData.insert_on_demand_alert(ts, site_id, reason, reporter, alert_level)

        if result['status']:
            od_data_return = {
                "ok": True,
                "message": "Successfully added new on demand data.",
                "data": result['data']
            }
        else:
            od_data_return = {
                "ok": False,
                "message": f"Failed to add OD data."
            }
        H.var_checker("od_data_return", od_data_return, True)
    except Exception as err:
        raise(err)
        od_data_return = {
            "ok": False,
            "message": f"Failed to add OD data."
        }
    return jsonify(od_data_return)


@ON_DEMAND_BLUEPRINT.route("/ground_data/on_demand/raise", methods=["POST"])
def raise_on_demand():
    try:
        print(request.get_json())
        (site_id, timestamp) = request.get_json().values()

        trigger_sym_id = AlertGen.get_operational_trigger_symbol(
                                    trigger_source='on demand',
                                    alert_level=1,
                                    return_col="trigger_sym_id")

        op_trig_data_dict = AlertGen.fetch_recent_operational_trigger(
            AlertGen,
            site_id=site_id,
            trig_sym_id=trigger_sym_id
        )
        H.var_checker("op_trig_data_dict", op_trig_data_dict, True)

        # If nothing exists in database:
        if not op_trig_data_dict:
            result = AlertGen.insert_operational_trigger(
                site_id=site_id,
                trig_sym_id=trigger_sym_id,
                ts_updated=timestamp
            )
        # Else update especially ts in database:
        else:
            trigger_id = op_trig_data_dict["trigger_id"]
            result = AlertGen.update_operational_trigger(
                op_trig_id=trigger_id,
                trig_sym_id=trigger_sym_id,
                ts_updated=timestamp
            )

        od_data_return = {
            "ok": True,
            "message": "Successfully added new on demand data."
        }
    except Exception as err:
        raise(err)
        od_data_return = {
            "ok": False,
            "message": f"Failed to add OD data."
        }
    return jsonify(od_data_return)

@ON_DEMAND_BLUEPRINT.route("/ground_data/moms/fetch/feature/<feature_id>/<site_id>", methods=["GET"])
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