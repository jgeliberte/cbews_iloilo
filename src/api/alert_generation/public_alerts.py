from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
from datetime import datetime as dt, timedelta
from src.model.alert_generation import AlertGeneration
from src.api.helpers import Helpers


PUBLIC_ALERTS_BLUEPRINT = Blueprint("public_alerts_blueprint", __name__)

def get_unique_trigger_per_type(trigger_list):
    """
    Util Function
    """

    new_trigger_list = []
    unique_triggers_set = set({})
    for trigger in trigger_list:
        (release_id, trigger_type, timestamp, info) = trigger

        if not trigger_type in unique_triggers_set:
            unique_triggers_set.add(trigger_type)
            new_trigger_list.append(trigger)

    return new_trigger_list


@PUBLIC_ALERTS_BLUEPRINT.route("/alert_gen/public_alerts/get_ongoing_and_extended_monitoring", methods=["GET"])
@PUBLIC_ALERTS_BLUEPRINT.route("/alert_gen/public_alerts/get_ongoing_and_extended_monitoring/<run_ts>", methods=["GET"])
def get_ongoing_and_extended_monitoring(run_ts=dt.now()):
    """
    return
    """
    ROU_EXT_RELEASE_TIME = 12
    release_interval_hours = 4
    extended_monitoring_days = 3

    try:
        AG = AlertGeneration
        H = Helpers
        events = AG.get_ongoing_extended_overdue_events(complete=True)

        active_events_dict = { 
            "latest": [], "extended": [], "overdue": [],
            "routine": {}
        }
        for e in events:
            try: 
                event_data = {}
                ( event_id, site_id, event_start,
                    latest_release_id, latest_trigger_id,
                    validity, status ) = e
                latest_release = AG.get_event_releases(event_id=event_id, return_count=1)
                ( release_id, data_timestamp,
                    internal_alert_level, release_time ) = latest_release[0]

                data_ts = data_timestamp
                rounded_data_ts = H.round_to_nearest_release_time(
                                        data_ts=data_ts,
                                        interval=release_interval_hours
                                    )
                
                if data_ts.hour == 23 and release_time.hour < release_interval_hours:
                    str_data_ts_ymd = dt.strftime(rounded_data_ts, "%Y-%m-%d")
                    str_release_time = str(release_time)
                    release_time = f"{str_data_ts_ymd} {str_release_time}"
                
                internal_alert = internal_alert_level
                event_data["data_ts"] = data_ts
                event_data["release_time"] = release_time
                event_data["internal_alert_level"] = internal_alert
                event_data["validity"] = validity

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

                ############
                # TRIGGERS #
                ############
                all_event_triggers = AG.get_event_triggers(
                    event_id=event_id
                )
                unique_triggers = get_unique_trigger_per_type(all_event_triggers)
                event_data["unique_triggers"] = unique_triggers

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

        surficial = {
            "status": True,
            "message": "Success",
            "data": active_events_dict
        }

    except Exception as err:
        raise err
        surficial = {
            "status": False,
            "message": f"Failed to fetch active events data. Error: {err}"
        }
    return jsonify(surficial)
