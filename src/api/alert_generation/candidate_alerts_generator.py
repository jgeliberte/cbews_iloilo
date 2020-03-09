import sys, json
from flask import Blueprint, jsonify, request
from connections import SOCKETIO
from datetime import datetime as dt, timedelta
from src.api.alert_generation.public_alerts import get_ongoing_and_extended_monitoring 
from src.model.users import Users
from src.api.helpers import Helpers as h


####################################
# process_with_alerts_entries fnxs #
####################################


def get_retrigger_index(retriggers, trigger):
    # const temp = retriggers.map(x => x.alert).indexOf(trigger);
    temp = next((index for (index, d) in enumerate(retriggers) if d["alert"] == trigger), -1)

    return temp


def adjust_alert_level_if_invalid_sensor(public_alert, entry):
    internal_alert = entry["internal_alert"] 
    subsurface_alert = entry["subsurface"]
    surficial_alert = entry["surficial"]
    retriggers = entry["triggers"]

    public_alert_level = None
    internal_alert_level = None
    invalid_index = None

    is_L2_available = get_retrigger_index(retriggers, "L2")
    if is_L2_available > -1 and public_alert == "A3":
        public_alert_level = "A2"
        # internal_alert_level = internal_alert.replace(/S0*/g, "s").replace("A3", "A2");
        # TODO: Test this code
        internal_alert_level = re.sub(
                    r"%s(0|x)?" % "S", "s", internal_alert)
        internal_alert_level = re.sub(
                    r"%s?" % "A3", "A2", internal_alert)
        invalid_index = get_retrigger_index(retriggers, "L3")
    else:
        public_alert_level = "A1"
        # TODO: Test this code
        internal_alert_level = re.sub(
                    r"%s?" % ["s", "S"], "", internal_alert)
        
        # TODO double check this from 236
        has_sensor_data = list(filter(lambda x: x["alert"] != "ND"), subsurface_alert)
        if not has_sensor_data and surficial_alert == "g0":
            # TODO doublecheck this 237
            internal_alert_level = re.sub(
                    r"A[1-3]?", "ND", internal_alert_level)
        else:
            internal_alert_level = re.sub(
                    r"A[1-3]?", "A1", internal_alert_level)

        if is_L2_available == -1:
            invalid_index = get_retrigger_index(retriggers, "L3")
        else:
            invalid_index = is_L2_available

    obj = {
        "public_alert": public_alert_level,
        "internal_alert": internal_alert_level,
        "invalid_index": invalid_index
    }

    return obj


def get_all_invalid_triggers_of_site(site_code, invalids_list):
    invalid_site_triggers_list = []
    invalid_site_triggers_list = list(filter(lambda x: x["site_code"] == site_code, invalids_list))

    return sorted(invalid_site_triggers_list, key=lambda x: x["alert"], reverse=True)


def process_with_alerts_entries(with_alerts, merged_list, invalids):
    candidates_list = []

    for w_alert in with_alerts:
        entry = w_alert
        site_code = entry["site_code"]
        internal_alert = entry["internal_alert"] 

        # TODO REVISIT CODE
        entry_source = internal_alert
        if "-" in internal_alert:
            entry_source = internal_alert.split("-", 1)

        entry["status"] = "valid"
        entry["invalid_list"] = []

        site_invalids_list = get_all_invalid_triggers_of_site(site_code, invalids)
        h.var_checker("site_invalids_list", site_invalids_list, True)

        is_valid_but_needs_manual = False
        for s_invalid in site_invalids_list:
            # TODO Manumanong code. Needs to be refactored.
            # alert_index = -1
            # for index, value in enumerate(merged_list):
            #     if value["site_code"] == s_invalid["site_code"]:
            #         alert_index = index
            #         break
            # End of experimental code
            alert_index = next((index for (index, d) in enumerate(merged_list) if d["site_code"] == s_invalid["site_code"]), -1)
            public_alert = s_invalid["alert"]

            invalid_trigger = s_invalid["trigger_source"]
            alerts_source_list = list(entry_source)

            # TODO Can use lambda para mas maikli
            temp = []
            for source in alerts_source_list:
                trig = None
                # TODO static code. must be dynamic codes and trigger source
                if source.upper() == "S":
                    trig = "subsurface"
                elif source.upper() == "R":
                    trig = "rainfall"
                temp.append(trig)

            for source in alerts_source_list:
                if source == invalid_trigger:
                    entry["invalid_list"].append(s_invalid)

                    # Check if alert exists on database
                    if alert_index == -1 and len(alerts_source_list) == 1:
                        entry["status"] = "invalid"
                    else:
                        entry["status"] = "partial"
                    
                    trigger_letter = "Z"
                    if source == "subsurface":
                        trigger_letter = "S"
                    elif source == "rainfall":
                        trigger_letter = "R"

                    is_present_on_proposed_internal_alert = False
                    is_R_present = False
                    is_S_present = -1
                    if alert_index > -1:
                        internal_alert_level = merged_list[alert_index]["internal_alert_level"]
                        # temp = 
                        up_t_letter = trigger_letter.upper()
                        low_t_letter = trigger_letter.lower()
                        is_present_on_proposed_internal_alert = up_t_letter in internal_alert or low_t_letter in internal_alert
                        is_R_present = "R" in internal_alert
                        is_S_present = "s" in internal_alert and "S" in internal_alert
                    
                    if alert_index == -1 or is_present_on_proposed_internal_alert:
                        return_dict = None
                        if source == "subsurface":
                            if is_S_present:
                                is_valid_but_needs_manual = True
                            else:
                                return_dict = adjust_alert_level_if_invalid_sensor(public_alert, entry)
                        elif source == "rainfall":
                            if is_R_present:
                                is_valid_but_needs_manual = True
                            else:
                                return_dict = adjust_alert_level_if_invalid_sensor(entry)

                        if return_dict:
                            invalid_index = return_dict["invalid_index"]
                            entry = entry.update(entry)
                            entry = entry.update(return_dict)
                            entry["triggers"]["invalid_index"]["invalid"] = True

        if len(entry["internal_alert"]) <= 3:
            entry["status"] = "invalid"

        if not is_valid_but_needs_manual:
            return_dict = get_latest_trigger(entry)
            entry = entry.update(entry)
            entry = entry.update(return_dict)

        for_updating = True
        index = next((index for (index, d) in enumerate(merged_list) if d["site_code"] == entry["site_code"]), -1)

        if index != -1:
            merged_list[index]["for_release"] = True

            data_timestamp = merged_list[index]["data_timestamp"]
            trigger_timestamp = merged_list[index]["trigger_timestamp"]
            latest_trigger_timestamp = entry["latest_trigger_timestamp"]
            ts = entry["ts"]

            if h.str_to_dt(data_timestamp) == h.str_to_dt(ts):
                for_updating = True

            # TODO Might fail when testing
            if h.str_to_dt(trigger_timestamp) >= h.str_to_dt(latest_trigger_timestamp):
                entry["latest_trigger_timestamp"] = "end"
                entry["trigger"] = "No new triggers"
            
            if is_valid_but_needs_manual:
                entry["latest_trigger_timestamp"] = "manual"
                entry["trigger"] = "manual"
                entry["validity"] = "manual"
                entry["is_manual"] = "manual"

        if for_updating:
            candidates_list.append(entry)

    return candidates_list

def separate_with_alerts_to_no_alerts_on_JSON(alerts_list):
    no_alerts = []
    with_alerts = []
    for alert in alerts_list:
        if alert["public_alert"] == "A0":
            no_alerts.append(alert)
        else:
            with_alerts.append(alert)
    
    return no_alerts, with_alerts


def process_candidate_alerts(generated_alerts, db_alerts):
    """
    """
    h.var_checker("db_alerts", db_alerts, True)
    all_alerts = generated_alerts["alerts"]
    invalids = generated_alerts["invalids"]

    latest = db_alerts["latest"]
    overdue = db_alerts["overdue"]
    extended = db_alerts["extended"]

    candidate_alerts_list = []

    no_alerts, with_alerts = separate_with_alerts_to_no_alerts_on_JSON(all_alerts)

    merged_list = latest.extend(overdue)

    return_list = process_with_alerts_entries(with_alerts, merged_list, invalids)
    invalid_entries = list(filter(lambda x: x["status"] == invalid, return_list))
    candidate_alerts_list.extend(return_list)
    
    return_list = tag_sites_for_lowering(merged_list, no_alerts)
    lowering_return, lowering_index = return_list
    candidate_alerts_list.extend(lowering_return)

    return_list = prepareSites


def main(internal_gen_data=None):
    generated_alerts_dict = []
    if internal_gen_data:
        generated_alerts_dict = internal_gen_data
    else:
        generated_alerts_dict = []
        full_filepath = "../../Documents/monitoringoutput/alertgen/PublicAlertRefDB.json"
        print(f"Getting data from {full_filepath}")
        print()

        with open(full_filepath) as json_file:
            generated_alerts_dict = json_file.read()
            generated_alerts_dict = json.loads(generated_alerts_dict)[0]

    db_alerts = get_ongoing_and_extended_monitoring(source="api")
    db_alerts = json.loads(db_alerts)["data"]

    h.var_checker("generated_alerts_dict", generated_alerts_dict, True)

    candidate_alerts_list = process_candidate_alerts(
        generated_alerts=generated_alerts_dict,
        db_alerts=db_alerts
    )





if __name__ == "__main__":
    main()