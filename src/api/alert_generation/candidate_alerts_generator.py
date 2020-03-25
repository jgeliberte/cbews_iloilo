import sys, json, os
from flask import Blueprint, jsonify, request
from connections import SOCKETIO
from datetime import datetime as dt, timedelta
from src.api.alert_generation import public_alerts as PA 
from config import APP_CONFIG
from src.model.alert_generation import AlertGeneration as AG
from src.model.users import Users
from src.model.sites import Sites
from src.api.helpers import Helpers as h


def prepare_sites_for_routine_release(no_alerts, excluded_indexes_list, invalid_entries):
    # Outside array pertains to season group [season 1, season 2]
    # Inside arrays contains months (0 - January, 11 - December)
    wet = [[0, 1, 5, 6, 7, 8, 9, 10, 11], [4, 5, 6, 7, 8, 9]]
    dry = [[2, 3, 4], [0, 1, 2, 3, 10, 11]]

    data_timestamp = no_alerts[0]["ts"]
    data_ts = h.str_to_dt(data_timestamp)
    weekday = data_ts.weekday()
    matrix = None
    return_list = []
    routine_list = []

    if dt.strftime(data_ts, "%H:%M") == "11:30":
        if weekday == 3:
            matrix = dry
        elif weekday in [2, 5]:
            matrix = wet
    
    if matrix:
        merged_list = no_alerts + invalid_entries
        for index, item in enumerate(merged_list):
            site_code = entry["site_code"]
            ts = entry["ts"]
            month = h.str_to_dt(ts).month
            site_detail = Sites.get_site_details(filter_value=site_code, site_filter="site_code")
            h.var_checker("site_detail", site_detail, True)
            season = site_detail["season"]

            if month in matrix[season - 1]:
                print("")
                # TODO: FINISH ROUTINE TASKS. TEST FOR NOW.


def prepare_sites_for_extended_release(extended_sites, no_alerts):
    """
    """
    return_list = []
    extended_index = []
    for site in extended_sites:
        index = next((index for (index, d) in enumerate(no_alerts) if d["site_code"] == site["site_code"]), -1)
        if index > -1:
            x = no_alerts[index]
            extended_index.append(index)

            data_ts = site["data_ts"]
            day = site["day"]
            ts = x["ts"]

            if data_ts != ts and day > 0:
                if ts.hour == 11 and ts.minute == 30:
                    x.update({
                        "status": "extended",
                        "latest_trigger_timestamp": "extended",
                        "trigger": "extended",
                        "validity": "extended"
                    })
                    x = prepare_candidate_for_release(x)
                    return_arr.append(x)

    return return_list, extended_index


def tag_sites_for_lowering(merged_list, no_alerts):
    """
    """
    return_arr = []
    lowering_index = []
    for site in merged_list:
        if not site["for_release"]:
            index = next((index for (index, d) in enumerate(no_alerts) if d["site_code"] == site["site_code"]), -1)
            x = no_alerts[index]
            lowering_index.append(index)
            data_ts = site["data_ts"]
            internal_alert_level = site["internal_alert_level"]

            if data_ts != x["ts"] and internal_alert_level not in ["A0", "ND"]:
                x.update({
                    "status": "valid",
                    "latest_trigger_timestamp": "end",
                    "trigger": "No new triggers",
                    "validity": "end"
                })

                x = prepare_candidate_for_release(x, merged_list)
                return_arr.append(x)
    return [return_arr, lowering_index]


####################################
# process_with_alerts_entries fnxs #
####################################


def get_latest_trigger(entry):
    """
    """
    retriggers = entry["triggers"]
    max = None
    for index, retrig in enumerate(retriggers):
        if max:
            ts = h.str_to_dt(max["ts"])
            if isinstance(max["ts"], str):
                ts = h.str_to_dt(max["ts"])

        if not max or ts < h.str_to_dt(retriggers[index]["ts"]):
            max = retriggers[index]
    return {
        "latest_trigger_timestamp": max["ts"],
        "trigger": max["alert"]
    }


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
            entry.update(return_dict)

        for_updating = True
        index = next((index for (index, d) in enumerate(merged_list) if d["site_code"] == entry["site_code"]), -1)

        if index != -1:
            merged_list[index]["for_release"] = True

            data_ts = merged_list[index]["data_ts"]
            trigger_timestamp = merged_list[index]["trigger_timestamp"]
            latest_trigger_timestamp = entry["latest_trigger_timestamp"]
            ts = entry["ts"]

            if h.str_to_dt(data_ts) == h.str_to_dt(ts):
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
            entry = prepare_candidate_for_release(entry, merged_list)
            candidates_list.append(entry)

    return candidates_list


def prepare_candidate_for_release(candidate, merged_list=None):
    """
    """
    site_code = candidate["site_code"]
    site_id = candidate["site_id"]
    new_data_ts = h.str_to_dt(candidate["ts"])

    ########################
    # CHECK IF NEW RELEASE $
    ########################
    # db_alert = next((index for (index, d) in enumerate(no_alerts) if d["site_code"] == site_code), None)
    db_alert = next(filter(lambda x: x['site_code'] == site_code, merged_list), None)
    is_new_release = True
    if db_alert:
        latest_saved_data_ts = h.str_to_dt(db_alert["data_ts"])
        is_ts_already_released = new_data_ts <= latest_saved_data_ts
        if is_ts_already_released:
            is_new_release = False

    #########################
    # CHECK IF RELEASE TIME #
    #########################
    scheduled_release_time = h.round_to_nearest_release_time(new_data_ts)
    start_ts = scheduled_release_time - timedelta(minutes=30)
    # NOTE: allows 8, 12, 16, 20, 23, 3 timestamp release
    is_release_time = False
    if start_ts < new_data_ts <= scheduled_release_time:
        is_release_time = True
    else:
        if is_new_release:
            is_release_time = True

    ###########################################
    # PREPARE TRIGGERS FOR RELEASE.           #
    # CHECK IF TRIGGERS WERE ALREADY RELEASED #
    ###########################################
    raw_triggers = candidate["triggers"]
    tech_info_dict = candidate["tech_info"]
    new_trigger_list = []
    for trigger in raw_triggers:
        source_id = trigger["source_id"]
        trigger_source = AG.get_trigger_hierarchy(source_id, "trigger_source")
        ots_symbol = trigger["alert"]
        trigger_type = AG.get_internal_alert_symbol_row(trigger_symbol=ots_symbol, return_col="ias.alert_symbol")
        tech_info = tech_info_dict[trigger_source]

        trigger_payload = {
            "trigger_type": trigger_type,
            "timestamp": trigger["ts"],
            "info": tech_info,
            "trigger_sym_id": trigger["trigger_sym_id"],
            "source_id": source_id,
            "alert_level": trigger["alert_level"],
            "trigger_id": trigger["trigger_id"],
            "ots_symbol": ots_symbol,
            "trigger_source": trigger_source
        }

        new_trigger_list.append(trigger_payload)



    # h.var_checker("event_triggers", event_triggers, True)
    # rel_trigs = identify_release_triggers(raw_triggers, tech_info)
    
    release_related_attributes = {
        "release_time": h.dt_to_str(dt.now()),
        "data_ts": candidate["ts"],
        "release_triggers": new_trigger_list,
        "public_alert_level": int(candidate["public_alert"][1]),
        "internal_alert": candidate["internal_alert"],
        "public_alert": candidate["public_alert"],
        "is_release_time": is_release_time, 
        "is_new_release": is_new_release
    }
    candidate.update(release_related_attributes)
    return candidate


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
    all_alerts = generated_alerts["alerts"]
    invalids = generated_alerts["invalids"]

    latest = db_alerts["latest"]
    overdue = db_alerts["overdue"]
    extended = db_alerts["extended"]

    candidate_alerts_list = []

    no_alerts, with_alerts = separate_with_alerts_to_no_alerts_on_JSON(all_alerts)

    merged_list = latest + overdue

    return_list = process_with_alerts_entries(with_alerts, merged_list, invalids)
    invalid_entries = list(filter(lambda x: x["status"] == "invalid", return_list))
    candidate_alerts_list.extend(return_list)
    
    return_list = tag_sites_for_lowering(merged_list, no_alerts)
    lowering_return, lowering_indexes_list = return_list
    candidate_alerts_list.extend(lowering_return)

    return_list = prepare_sites_for_extended_release(extended, no_alerts)
    extended_return, extended_indexes_list = return_list
    candidate_alerts_list.extend(extended_return)

    excluded_indexes_list = lowering_indexes_list + extended_indexes_list
    if no_alerts:
        return_list = prepare_sites_for_routine_release(no_alerts, extended_indexes_list, invalid_entries)

    return candidate_alerts_list


def main(internal_gen_data=None):
    start_time = dt.now()
    generated_alerts_dict = []
    if internal_gen_data:
        generated_alerts_dict = internal_gen_data
    else:
        generated_alerts_dict = []
        full_filepath = "/home/louie-cbews/CODES/cbews_iloilo/Documents/monitoringoutput/alertgen/PublicAlertRefDB.json"
        print(f"Getting data from {full_filepath}")
        print()

        with open(full_filepath) as json_file:
            generated_alerts_dict = json_file.read()
            generated_alerts_dict = json.loads(generated_alerts_dict)[0]

    db_alerts = PA.get_ongoing_and_extended_monitoring(source="api")
    db_alerts = json.loads(db_alerts)["data"]

    candidate_alerts_list = process_candidate_alerts(
        generated_alerts=generated_alerts_dict,
        db_alerts=db_alerts
    )

    json_data = json.dumps(candidate_alerts_list)

    directory = APP_CONFIG['CANDIDATE_DIR']
    # h.var_checker("directory", directory, True)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(directory + "/candidate_alerts.json", "w") as file_path:
        file_path.write(json_data)

    print(f"candidate_alerts.json written at {directory}")
    print('runtime = %s' %(dt.now() - start_time))

    return json_data

if __name__ == "__main__":
    main()