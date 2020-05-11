from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
from src.api.helpers import Helpers as H

class PublicAlertTable():

    def fetch_latest_event(self, site_id, return_col=None):
        """
        """
        option = "*"
        if return_col:
            option = return_col
        query = f"SELECT {option} FROM public_alert_event "
        query += f"WHERE site_id = {site_id} ORDER BY event_id DESC LIMIT 1"

        schema = "senslopedb"
        event = DB.db_read(query, schema)

        return_data = None
        if event:
            if return_col:
                return_data = event[0][0]
            else:
                temp = event[0]
                return_data = {
                    "event_id": temp[0],
                    "site_id": temp[1],
                    "event_start": temp[2],
                    "latest_release_id": temp[3],
                    "latest_trigger_id": temp[4],
                    "validity": temp[5],
                    "status": temp[6] 
                } 

        return return_data


    def insert_public_alert_event(self, site_id, event_start, latest_rel_id,
                                latest_trig_id, validity, status):
        """
        """
        try:
            query = "INSERT INTO public_alert_event "
            query += "(site_id, event_start, latest_release_id, latest_trigger_id, validity, status) "
            query += f"VALUES ({site_id}, '{event_start}', null, null, '{validity}', '{status}')"

            schema = "senslopedb"
            event_id = DB.db_modify(query, schema, True)
        except Exception as err:
            raise(err)

        return event_id


    def update_public_alert_event(self, update_dict, where_dict):
        """
        """
        try:
            query = "UPDATE public_alert_event "
            query += "SET "
            length = len(update_dict.items())
            for index, item in enumerate(update_dict.items()):
                value = item[1]
                if isinstance(value, str) or isinstance(value, dt):
                    value = f"'{value}'"
                query += f"{item[0]} = {value} "
                if index + 1 < length:
                    query += ", "

            index = 0
            for item in where_dict.items():
                sql = "AND "
                if index == 0:
                    sql = "WHERE "
                query += f"{sql}{item[0]} = {item[1]} "

            schema = "senslopedb"
            result = DB.db_modify(query, schema, True)

            if result:
                result = result[0]
        except Exception as err:
            raise(err)

        return result


    def insert_public_alert_release(self, event_id, data_ts, internal_alert,
                                release_time, comments, bulletin_number,
                                reporter_id_mt, reporter_id_ct):
        """
        """
        query = "INSERT INTO public_alert_release "
        query += "(event_id, data_timestamp, internal_alert_level, release_time, "
        query += "comments, bulletin_number, reporter_id_mt, reporter_id_ct) "
        query += f"VALUES ({event_id}, '{data_ts}', '{internal_alert}', "
        query += f"'{release_time}', '{comments}', {bulletin_number}, "
        query += f"{reporter_id_mt}, {reporter_id_ct})"

        schema = "senslopedb"
        result = DB.db_modify(query, schema, True)

        if result:
            result = int(result)

        return result


    def insert_public_alert_trigger(self, event_id, release_id, 
                                    trigger_type, timestamp, info):
        """
        """
        query = "INSERT INTO public_alert_trigger "
        query += "(event_id, release_id, trigger_type, timestamp, info) "
        query += f"VALUES ({event_id}, {release_id}, '{trigger_type}', '{timestamp}', '{info}')"

        schema = "senslopedb"
        result = DB.db_modify(query, schema, True)
    
        if result:
            pub_trigger_id = int(result)


        return pub_trigger_id


    def fetch_site_bulletin_number(self, site_id):
        """
        """
        query = "SELECT bulletin_number FROM bulletin_tracker "
        query += f"WHERE site_id = {site_id}"
        schema = "senslopedb"
        bulletin_number = DB.db_read(query, schema)
        if bulletin_number:
            bulletin_number = int(bulletin_number[0][0])

        return bulletin_number


    def update_bulletin_number(self, site_id, bulletin_number):
        """
        """
        query = "UPDATE bulletin_tracker "
        query += f"SET bulletin_number = {bulletin_number} "
        query += f"WHERE site_id = {site_id}"

        schema = "senslopedb"
        result = DB.db_modify(query, schema, True)

        return result

