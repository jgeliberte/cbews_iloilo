from flask import Blueprint, jsonify, request
from connections import SOCKETIO
import sys
from datetime import datetime as dt, timedelta
from src.model.alert_generation import AlertGeneration
from src.model.users import Users
from src.api.helpers import Helpers


def process_candidate_alerts(generated_alerts,)


def main(internal_gen_data):
    generated_alerts_dict = []
    if internal_gen_data:
        generated_alerts_dict = internal_gen_data
    else:
        generated_alerts_dict = []
        full_filepath = "../../../Documents/monitoringoutput/alertgen/PublicAlertRefDB.json"
        print(f"Getting data from {full_filepath}")
        print()

        with open(full_filepath) as json_file:
            generated_alerts_dict = json_file.read()

    db_alerts = AlertGeneration.get_ongoing_extended_overdue_events()

    Helpers.var_checker("alerts", alerts, True)
    Helpers.var_checker("invalids", invalids, True)
    Helpers.var_checker("db_alerts", db_alerts, True)

    candidate_alerts_list = process_candidate_alerts(
        generated_alerts=generated_alerts_dict,
        db_alerts=db_alerts
    )


