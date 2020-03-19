from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys, json
from datetime import datetime as dt, timedelta
from src.model.alert_generation import AlertGeneration
from src.model.public_alert_table import PublicAlertTable as PAT
from src.model.users import Users
from src.api.helpers import Helpers as h


PUBLIC_ALERTS_BLUEPRINT = Blueprint("public_alerts_blueprint", __name__)


def get_trigger_type_details(trigger_type):
    """
    Util Function
    """
    print()


def format_release_triggers(payload, process_one=False):
    """
    Util Function
    """
    AG = AlertGeneration
    release_trig_list = []

    list_to_process = payload
    if process_one:
        list_to_process = [payload]

    for trig in list_to_process:
        h.var_checker("trig", trig, True)
        (trigger_id, release_id, trigger_type, timestamp, info) = trig[0]
        trigger_source = AG.get_internal_alert_symbol_row(trigger_type, return_col="trigger_source")
        alert_level = AG.get_internal_alert_symbol_row(trigger_type, return_col="alert_level")
        release_trig_list.append({
            "trigger_id": trigger_id,
            "release_id": release_id,
            "trigger_type": trigger_type,
            "trigger_source": trigger_source,
            "trigger_alert_level": alert_level,
            # NOTE: Unconventional
            "trigger_timestamp": dt.strftime(timestamp, "%Y-%m-%d %H:%M:%S"), 
            "info": info
        })

    if process_one:
        return release_trig_list[0]
    else:
        return release_trig_list


def get_unique_trigger_per_type(trigger_list):
    """
    Util Function
    """
    AG = AlertGeneration
    new_trigger_list = []
    unique_triggers_set = set({})
    for trigger in trigger_list:
        (trigger_id, release_id, trigger_type, timestamp, info) = trigger
        trigger_source = AG.get_internal_alert_symbol_row(trigger_type, return_col="trigger_source")
        alert_level = AG.get_internal_alert_symbol_row(trigger_type, return_col="alert_level")

        if not trigger_type in unique_triggers_set:
            unique_triggers_set.add(trigger_type)
            new_trigger_list.append({
                "trigger_id": trigger_id,
                "release_id": release_id,
                "trigger_type": trigger_type,
                "trigger_source": trigger_source,
                "trigger_alert_level": alert_level,
                "timestamp": dt.strftime(timestamp, "%Y-%m-%d %H:%M:%S"), 
                "info": info
            })

    return new_trigger_list


@PUBLIC_ALERTS_BLUEPRINT.route("/alert_gen/public_alerts/get_ongoing_and_extended_monitoring", methods=["GET"])
@PUBLIC_ALERTS_BLUEPRINT.route("/alert_gen/public_alerts/get_ongoing_and_extended_monitoring/<run_ts>", methods=["GET"])
def get_ongoing_and_extended_monitoring(run_ts=dt.now(), source="fetch"):
    """
    return
    """
    ROU_EXT_RELEASE_TIME = 12
    release_interval_hours = 4
    extended_monitoring_days = 3

    try:
        AG = AlertGeneration
        events = AG.get_ongoing_extended_overdue_events(complete=True, include_site=True)

        active_events_dict = { 
            "latest": [], "extended": [], "overdue": [],
            "routine": {}
        }
        for e in events:
            try: 
                event_data = {}
                ( event_id, site_id, event_start,
                    latest_release_id, latest_trigger_id,
                    validity, status, site_code ) = e
                latest_release = AG.get_event_releases(event_id=event_id, return_count=1)
                ( release_id, data_timestamp,
                    internal_alert_level, release_time, reporter_id_mt ) = latest_release[0]
                release_time = str(release_time)
                ( firstname, lastname ) = Users.fetch_user(user_id=reporter_id_mt)[0]
                reporter = f"{firstname} {lastname}"

                data_ts = data_timestamp
                rounded_data_ts = h.round_to_nearest_release_time(
                                        data_ts=data_ts,
                                        interval=release_interval_hours
                                    )
                
                if data_ts.hour == 23 and release_time.hour < release_interval_hours:
                    str_data_ts_ymd = dt.strftime(rounded_data_ts, "%Y-%m-%d")
                    str_release_time = str(release_time)
                    release_time = f"{str_data_ts_ymd} {str_release_time}"
                
                h.var_checker("release_time", release_time, True)
                
                internal_alert = internal_alert_level
                event_data["site_id"] = site_id
                event_data["site_code"] = site_code
                # NOTE: Unconventional
                event_data["latest_release_id"] = release_id
                event_data["data_ts"] = dt.strftime(data_ts, "%Y-%m-%d %H:%M:%S")
                event_data["release_time"] = release_time
                event_data["internal_alert_level"] = internal_alert
                event_data["event_start"] = dt.strftime(event_start, "%Y-%m-%d %H:%M:%S")
                event_data["validity"] = dt.strftime(validity, "%Y-%m-%d %H:%M:%S")
                event_data["reporter"] = reporter

                if internal_alert[0] == "A":
                    # Probably A0, A1-..., A2-..., A3-...
                    # TODO: Create a model function checking for the corresponding 
                    # alert level of A1, A2, A3
                    public_alert_level = internal_alert[1]
                elif internal_alert[0] == "N":
                    public_alert_level = 1
                else:
                    raise Exception("Unknown alert level in get ongoing fnx")
                event_data["public_alert_level"] = public_alert_level
                event_data["recommended_response"] = AG.get_public_alert_symbols_row(
                    alert_level=public_alert_level,
                    return_col="recommended_response"
                )

                ###########################
                # LATEST RELEASE TRIGGERS #
                ###########################
                latest_release_trigger = AG.get_release_triggers(
                    release_id=release_id,
                    return_count=1
                )
                # event_data["release_triggers"] = format_release_triggers(payload=latest_release_trigger, process_one=True)
                temp = format_release_triggers(payload=latest_release_trigger, process_one=True)
                event_data.update(temp)

                #########################
                # LATEST EVENT TRIGGERS #
                #########################
                all_event_triggers = AG.get_event_triggers(
                    event_id=event_id
                )
                event_data["latest_event_triggers"] = get_unique_trigger_per_type(all_event_triggers)

                if run_ts <= validity:
                    active_events_dict["latest"].append(event_data)
                    print("Seeing a latest")
                elif validity < run_ts:
                    if int(event_data["public_alert_level"]) > 0:
                        print("Seeing an overdue")
                        # Late release
                        active_events_dict["overdue"].append(event_data)
                    else:
                        print("Seeing an extended")
                        # Get Next Day 00:00
                        next_day = validity + timedelta(days=1)
                        start = dt(next_day.year, next_day.month,
                                        next_day.day, 0, 0, 0)
                        # Day 3 is the 3rd 12-noon from validity
                        end = start + timedelta(days=extended_monitoring_days)
                        current = run_ts
                        # Count the days distance between current date and
                        # day 3 to know which extended day it is
                        difference = end - current
                        day = extended_monitoring_days - difference.days
                        print(day)

                        if day <= 0:
                            print("Seeing a latest")
                            active_events_dict["latest"].append(event_data)
                        elif day > 0 and day <= extended_monitoring_days:
                            event_data["day"] = day
                            print("Seeing an extended it is")
                            active_events_dict["extended"].append(event_data)
                        else:
                            print("FINISH EVENT")
                            # TODO: Create a model updating event row to finished status
                            # update_public_alert_event_status(status="finished")

            except Exception as err:
                raise err

        alerts_list = {
            "status": True,
            "message": "Success",
            "data": active_events_dict
        }
        h.var_checker("active_events_dict", active_events_dict, True)

    except Exception as err:
        raise err
        alerts_list = {
            "status": False,
            "message": f"Failed to fetch active events data. Error: {err}"
        }

    if source == "fetch":
        return jsonify(alerts_list)
    else:
        return json.dumps(alerts_list)


#############################
# insert_ewi util functions #
#############################

@PUBLIC_ALERTS_BLUEPRINT.route("/alert_gen/public_alerts/insert_ewi", methods=["POST"])
def adjust_bulletin_number(site_id):
    """
    Returns updated bulletin number.
    """
    try:
        bulletin_number = PAT.fetch_site_bulletin_number(site_id=50)
        H.var_checker("bulletin_number", bulletin_number, True)
        new_bulletin_number = PAT.update_bulletin_number(site_id=50, bulletin_number=bulletin_number+1)
        H.var_checker("new_bulletin_number", new_bulletin_number, True)

        response = {
            "status": True,
            "data": moms_list
        }
    except Exception as err:
        print(err)
        response = {
            "status": False,
            "data": moms_list
        }
    return "new_bulletin_number"


@PUBLIC_ALERTS_BLUEPRINT.route("/alert_gen/public_alerts/insert_ewi", methods=["POST"])
def insert_ewi():
    """
    """
    json_data = request.get_json()
    h.var_checker("insert_ewi json_data", json_data, True)

    try:
        site_id = json_data["site_id"]
    except KeyError:
        site_code = json_data["site_code"]
        # TODO
        site_id = 50

    data_ts = json_data["data_ts"]
    release_time = json_data["release_time"]
    comments = json_data["comments"]
    reporter_id_mt = json["reporter_id_mt"]
    reporter_id_ct = json["reporter_id_ct"]

    release_id = None
    validity = None
    update_event_container = {}
    # For routine sites
    release_list = []

    release_dict = {
        "data_ts": data_ts, 
        "release_time": release_time,
        "comments": comments,
        "reporter_id_mt": reporter_id_mt,
        "reporter_id_ct": reporter_id_ct
    }

    status = json_data["status"]
    if status == "routine":
        for routine_entry in json_data["routine_list"]:
            event_id = PAT.insert_public_alert_event(
                site_id=site_id, event_start=data_ts, latest_rel_id=None,
                latest_trig_id=None, validity=validity, status=status
            )
            H.var_checker("event_id", event_id, True)

            release_dict["event_id"] = event_id
            release_dict["internal_alert_level"] = routine_entry["internal_alert"]
            release_dict["bulletin_number"] = adjust_bulletin_number(site_id=site_id)

            release_list.append(release_dict)
    
    else:
        release_dict["internal_alert_level"] = json_data["internal_alert_level"]
        release_dict["bulletin_number"] = json_data["bulletin_number"]

        if status == "new":
            event_id = PAT.insert_public_alert_event(
                site_id=site_id, event_start=data_ts, latest_rel_id=None,
                latest_trig_id=None, validity=validity, status=status
            )
            release_dict["event_id"] = event_id
            try:
                previous_event_id = json_data["previous_event_id"]
                if previous_event_id:
                    PAT.update_public_alert_event({
                        "status": "finished"
                    }, {
                        "event_id": previous_event_id
                    })
            except KeyError:
                pass
                
        else:
            event_id = json_data["current_event_id"]
            release_dict["event_id"] = event_id

            validity = AlertGeneration.get_public_alert_event_validity(event_id)

            if status in ["extended", "invalid", "finished"]:
                update_event_container["status"] = status
        
        release_list.append(release_dict)
    
    for release_dict in release_list:
        release_id = PAT.insert_public_alert_release(
            event_id=release_dict["event_id"],
            data_ts=release_dict["data_ts"],
            internal_alert=release_dict["internal_alert_level"],
            release_time=release_dict["release_time"],
            comments=release_dict["comments"],
            bulletin_number=release_dict["bulletin_number"],
            reporter_id_mt=release_dict["reporter_id_mt"],
            reporter_id_ct=release_dict["reporter_id_ct"]
        )

        if status == "routine":
            event_id = release_dict["event_id"]
        elif status in ["new", "on-going"]:
            if "extend_ND" in json_data or "extend_rain_x" in json_data:
                update_event_container["validity"] = h.str_to_dt(validity) + timedelta(hours=4)
            else:
                return_list
