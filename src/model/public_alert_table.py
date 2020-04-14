from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
from src.api.helpers import Helpers as H

class PublicAlertTable():

    def insert_public_alert_event(self, site_id, event_start, latest_rel_id,
                                latest_trig_id, validity, status):
        """
        """
        query = "INSERT INTO public_alert_event "
        query += "(site_id, event_start, latest_release_id, latest_trigger_id, validity, status) "
        query += f"VALUES ({site_id}, '{event_start}', null, null, '{validity}', '{status}')"

        schema = "senslopedb"
        event_id = DB.db_modify(query, schema, True)

        return event_id


    def update_public_alert_event(self, update_dict, where_dict):
        """
        """
        query = "UPDATE public_alert_event "
        query += "SET "
        length = len(update_dict.items())
        for index, item in enumerate(update_dict.items()):
            value = item[1]
            if isinstance(item[1], str):
                value = f"'{item[1]}'"
            query += f"{item[0]} = {value}"
            if index + 1 < length:
                query += ", "

        index = 0
        for item in where_dict.items():
            sql = "AND "
            if index == 0:
                sql = "WHERE "
            query += f"{sql}{item[0]} = {item[1]} "

        schema = "senslopedb"
        event_id = DB.db_modify(query, schema, True)

        return event_id


    def insert_public_alert_release(self, event_id, data_ts, internal_alert,
                                release_time, comments, bulletin_number,
                                reporter_id_mt, reporter_id_ct):
        """
        """
        query = "INSERT INTO public_alert_release "
        query += "(event_id, data_timestamp, internal_alert_level, release_time, "
        query += "comments, bulletin_number, reporter_id_mt, reporter_id_ct) "
        query += f"VALUES ({event_id}, '{data_ts}', '{internal_alert}', "
        query += f"'{release_time}', '{comments}', {bulletin_number},"
        query += f"{reporter_id_mt}, {reporter_id_ct})"

        schema = "senslopedb"
        release_id = DB.db_modify(query, schema, True)

        return release_id


    def insert_public_alert_trigger(self, event_id, release_id, 
                                    trigger_type, timestamp, info):
        """
        """
        query = "INSERT INTO public_alert_trigger "
        query += "(event_id, release_id, trigger_type, timestamp, info) "
        query += f"VALUES ({event_id}, {release_id}, {trigger_type}, {timestamp}, {info})"

        schema = "senslopedb"
        pub_trigger_id = DB.db_modify(query, schema, True)

        return pub_trigger_id


    def fetch_site_bulletin_number(self, site_id):
        """
        """
        query = "SELECT bulletin_number FROM bulletin_tracker "
        query += f"WHERE site_id = {site_id})"

        schema = "senslopedb"
        bulletin_number = DB.db_read(query, schema)[0]

        return bulletin_number


    def update_bulletin_number(self, site_id, bulletin_number):
        """
        """
        query = "UPDATE bulletin_tracker "
        query += f"SET bulletin_number = {bulletin_number} "
        query += f"WHERE site_id = {site_id})"

        schema = "senslopedb"
        result = DB.db_modify(query, schema, True)

        return result

