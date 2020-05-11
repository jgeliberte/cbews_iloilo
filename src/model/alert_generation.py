from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
from src.api.helpers import Helpers as h


class AlertGeneration():

    def insert_alert_status(self,
                            trigger_id, ts_last_retrigger, ts_set,
                            ts_ack, alert_status, remarks,
                            user_id=1):
        """Insert alert status entry for operational triggers. 

        Args:
            trigger_id (int) -> operational trigger id
            ts_last_trigger (str dt) -> ts_updated from op trigger
            ts_set (str dt) -> ts where alert status was changed
            ts_ack (str dt) -> same ts as previous (was the "validating" set ts before)
            alert_status (int) -> 1: valid, 0: validating, -1: invalid
            remarks (str) -> info on validation
            user_id (int) -> who validated
        """
        print("INSERT ALERT STATUS")
        query = "INSERT INTO alert_status "
        query += "(ts_last_retrigger, trigger_id, ts_set, "
        query += "ts_ack, alert_status, remarks, user_id) "
        query += f"VALUES ('{ts_last_retrigger}', {trigger_id}, '{ts_set}', "
        query += f"'{ts_ack}', {alert_status}, '{remarks}', {user_id})"

        schema = "senslopedb"
        alert_id = DB.db_modify(query, schema, True)

        return alert_id

    def update_alert_status(self, update_dict, where_dict):
        """
        """
        print("UPDATE ALERT STATUS")
        try:
            query = "UPDATE alert_status "
            query += "SET "
            length = len(update_dict.items())
            for index, item in enumerate(update_dict.items()):
                value = item[1]
                if isinstance(item[1], str):
                    value = f"'{item[1]}'"
                query += f"{item[0]} = {value}"
                if index + 1 < length:
                    query += ", "
                # TODO: UPDATE TO OTHER UPDATE FF 2 lines
                elif index + 1 == length:
                    query += " "

            index = 0
            for item in where_dict.items():
                sql = "AND "
                if index == 0:
                    sql = "WHERE "
                query += f"{sql}{item[0]} = {item[1]} "

            schema = "senslopedb"
            alert_id = DB.db_modify(query, schema, True)
        except Exception as err:
            raise(err)

        return alert_id

    def fetch_alert_status(self, trigger_id):
        """
        """
        stat_row = None
        retun_dict = None
        query = "SELECT * FROM alert_status "
        query += f"WHERE trigger_id = {trigger_id}"

        schema = "senslopedb"
        stat_row = DB.db_read(query, schema)

        if stat_row:
            stat_row = stat_row[0]

            retun_dict = {
                "stat_id": stat_row[0],
                "ts_last_retrigger": h.dt_to_str(stat_row[1]),
                "trigger_id": stat_row[2],
                "ts_set": h.dt_to_str(stat_row[3]),
                "ts_ack": h.dt_to_str(stat_row[4]),
                "alert_status": stat_row[5],
                "remarks": stat_row[6],
                "user_id": stat_row[7]
            }

        return retun_dict

    def insert_operational_trigger(site_id, trig_sym_id, ts_updated):
        """Inserts operational_trigger table entry.

        Args:
            site_id (int) - where will the trigger be associated
            trig_sym_id (int) - trigger_sym_id - the kind of trigger
            ts_updated (str/datetime) - since this is an insert, we can assume
                            the ts and ts_updated is the same.
        """
        if isinstance(ts_updated, str):
            ts_updated = h.str_to_dt(ts_updated)
        ts = ts_updated
        query = "INSERT INTO senslopedb.operational_triggers "
        query += "(ts, site_id, trigger_sym_id, ts_updated) "
        query += f"VALUES ('{ts}', {site_id}, {trig_sym_id}, '{ts_updated}')"

        schema = "senslopedb"
        trigger_id = DB.db_modify(query, schema, True)

        return trigger_id

    def update_operational_trigger(op_trig_id, trig_sym_id, ts_updated):
        """
        Updates operational_trigger table entry: trigger_sym_id or ts_updated.

        Args:
            op_trig_id (int) - trigger_id identified the row to be updated
            trig_sym_id (int) - trigger_sym_id - the kind of trigger
            ts_updated (str/datetime) - updating ts_updated
        """
        query = f"UPDATE senslopedb.operational_triggers "
        query += f"SET trigger_sym_id={trig_sym_id}, ts_updated='{ts_updated}' "
        query += f"WHERE trigger_id = {op_trig_id}"


        schema = "senslopedb"
        result = DB.db_modify(query, schema, True)

        return result

    def fetch_recent_operational_trigger(self, site_id, trig_sym_id=None):
        """
        Returns most recent operational_trigger.

        Args:
            site_id (int) - trigger_id identified the row to be updated
            trig_sym_id (int) - trigger_sym_id - the kind of trigger
        """
        query = "SELECT * FROM operational_triggers "
        query += f"WHERE site_id = {site_id} "
        if trig_sym_id:
            query += f"AND trigger_sym_id = {trig_sym_id} "
        query += "ORDER BY ts_updated DESC"

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        return_dict = None
        if result:
            result = result[0]

            return_dict = {
                "trigger_id": result[0],
                "ts": result[1],
                "site_id": result[2],
                "trigger_sym_id": result[3],
                "ts_updated": result[4]
            }
        return return_dict


    def get_ongoing_extended_overdue_events(site_id=None, complete=False, include_site=False):
        """
        Returns ongoing, extended, and routine events
        """
        select_option = "site_id, status"
        if complete:
            select_option = "public_alert_event.*"

        select_option = f"{select_option}, sites.site_code " if include_site else select_option

        query = f"SELECT {select_option} FROM public_alert_event"
        if include_site:
            query = f"{query} INNER JOIN commons_db.sites USING (site_id)"
        query = f"{query} WHERE status in ('on-going', 'extended')"

        # schema = DB.db_switcher(site_id)
        schema = "senslopedb"
        result = DB.db_read(query, schema)
        return result

    def get_public_alert_event(event_id, include_site=False):
        """
        Returns event row. There is an option to include site details
        """
        query = "SELECT public_alert_event.*"
        if include_site:
            query = f"{query}, commons_db.sites.*"
        query = f"{query} FROM public_alert_event"
        if include_site:
            query = f"{query} INNER JOIN commons_db.sites USING (site_id)"
        query = f"{query} WHERE public_alert_event.event_id = {event_id}"

        # schema = DB.db_switcher("senslopedb")
        schema = "senslopedb"
        result = DB.db_read(query, schema)

        return result

    def get_public_alert_event_validity(event_id, include_site=False):
        """
        Returns event row. There is an option to include site details
        """
        query = f"SELECT validity FROM public_alert_event \
            WHERE public_alert_event.event_id = {event_id}"

        # schema = DB.db_switcher(site_id)
        schema = "senslopedb"
        result = DB.db_read(query, schema)

        if result:
            result = result[0][0]

        return result

    def get_event_releases(event_id, sort_order="desc", return_count=None, complete=False):
        """
        Returns public_alert_releases row/s
        """
        select_option = "release_id, data_timestamp, internal_alert_level, release_time, reporter_id_mt"
        if complete:
            select_option = "*"

        query = f"SELECT {select_option} FROM public_alert_release \
                WHERE event_id = {event_id} "

        order = "ASC" if sort_order in ["asc", "ASC"] else "DESC"
        query = f"{query} ORDER BY release_id {order}"

        query = f"{query} LIMIT {return_count}" if return_count else query

        # schema = DB.db_switcher(site_id)
        schema = "senslopedb"
        result = DB.db_read(query, schema)

        return_data = None
        if result:
            result = result[0]
            if complete:
                return_data = {
                    "release_id": result[0], 
                    "event_id": result[1], 
                    "data_timestamp": h.dt_to_str(result[2]), 
                    "internal_alert_level": result[3], 
                    "release_time": h.timedelta_to_str(result[4]),
                    "comments": result[5],
                    "bulletin_number": result[6],
                    "reporter_id_mt": result[7],
                    "reporter_id_ct": result[8]
                }
            else:
                return_data = {
                    "release_id": result[0], 
                    "data_timestamp": h.dt_to_str(result[1]), 
                    "internal_alert_level": result[2], 
                    "release_time": h.timedelta_to_str(result[3]), 
                    "reporter_id_mt": result[4]
                }

        return return_data


    def get_event_triggers(event_id, sort_order="desc", return_count=None, complete=False):
        """
        Returns public_alert_trigger row/s
        """
        select_option = "trigger_id, release_id, trigger_type, timestamp, info"
        if complete:
            select_option = "*"

        query = f"SELECT {select_option} FROM public_alert_trigger \
                WHERE event_id = {event_id}"
        order = "ASC" if sort_order in ["asc", "ASC"] else "DESC"
        query = f"{query} ORDER BY timestamp {order}"

        query = f"{query} LIMIT {return_count}" if return_count else query

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        temp_list = []
        if result:
            for trig in result:
                temp_dict = None
                if complete:
                    temp_dict = {
                        "trigger_id": trig[0],
                        "event_id": trig[1],
                        "release_id": trig[2],
                        "trigger_type": trig[3],
                        "timestamp": h.dt_to_str(trig[4]),
                        "info": trig[5]
                    }
                else:
                    temp_dict = {
                        "trigger_id": trig[0],
                        "release_id": trig[1],
                        "trigger_type": trig[2],
                        "timestamp": h.dt_to_str(trig[3]),
                        "info": trig[4]
                    }
                temp_list.append(temp_dict)

        return temp_list


    def get_release_triggers(release_id, sort_order="desc", return_count=None, complete=False):
        """
        Returns public_alert_trigger row/s
        """
        select_option = "trigger_id, release_id, trigger_type, timestamp, info"
        if complete:
            select_option = "*"

        query = f"SELECT {select_option} FROM public_alert_trigger \
                WHERE release_id = {release_id}"
        order = "ASC" if sort_order in ["asc", "ASC"] else "DESC"
        query = f"{query} ORDER BY timestamp {order}"

        query = f"{query} LIMIT {return_count}" if return_count else query

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        return result

    ###################################
    # MINI QUERIES
    ###################################

    def get_public_alert_symbols_row(alert_level=None, alert_symbol=None, return_col=None):
        """
        Returns public_alert_symbols row or value itself
        """
        select_option = "*"
        if return_col:
            select_option = return_col
        query = f"SELECT {select_option} FROM public_alert_symbols WHERE "

        # Either you give level or symbol. Pretty obvious one.
        if alert_level != None:
            query = f"{query} alert_level = {alert_level}"
        elif alert_symbol:
            query = f"{query} alert_symbol = {alert_symbol}"

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        return_data = None
        if return_col:
            return_data = result[0][0]
        else:
            temp = result[0]
            return_data = {
                "pub_sym_id": temp[0],
                "alert_symbol": temp[1],
                "alert_level": temp[2],
                "alert_type": temp[3],
                "recommended_response": temp[4]
            }

        return return_data

    def get_ias_table():
        """
        Util miniquery
        """
        select_option = f"internal_sym_id, \
                    ias.trigger_sym_id, \
                    ias.alert_symbol as ias_symbol, \
                    ots.alert_symbol as ots_symbol, \
                    ots.alert_description, \
                    alert_level, \
                    trigger_source"
        query = f"SELECT {select_option} FROM " + \
                "senslopedb.internal_alert_symbols ias " + \
            "INNER JOIN " + \
                "operational_trigger_symbols ots USING (trigger_sym_id) " + \
            "INNER JOIN " + \
                "trigger_hierarchies th USING (source_id)"

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        result_list = []
        for row in result:
            result_list.append({
                "trigger_sym_id": row[0],
                "internal_sym_id": row[1],
                "ias_symbol": row[2],
                "ots_symbol": row[3],
                "alert_description": row[4],
                "alert_level": row[5],
                "trigger_source": row[6]
            })

        return result_list


    def get_ias_by_lvl_source(trigger_source, alert_level, return_col=None):
        """
        Util miniquery
        """
        select_option = f"internal_sym_id, \
                    ias.trigger_sym_id, \
                    ias.alert_symbol as ias_symbol, \
                    ots.alert_symbol as ots_symbol, \
                    ots.alert_description, \
                    ots.alert_level, \
                    trigger_source"
        if return_col:
            select_option = return_col
        query = f"SELECT {select_option} FROM " + \
                "senslopedb.internal_alert_symbols ias " + \
            "INNER JOIN " + \
                "operational_trigger_symbols ots USING (trigger_sym_id) " + \
            "INNER JOIN " + \
                "trigger_hierarchies th USING (source_id)"

        query = f"{query} WHERE trigger_source = '{trigger_source}' "
        query = f"{query} AND alert_level = {alert_level}"

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        if return_col:
            result = result[0][0]

        return result


    def get_internal_alert_symbol_row(trigger_type=None, trigger_symbol=None, return_col=None):
        """
        Util miniquery
        """
        select_option = f"internal_sym_id, \
                    ias.trigger_sym_id, \
                    ias.alert_symbol as ias_symbol, \
                    ots.alert_symbol as ots_symbol, \
                    ots.alert_description, \
                    ots.alert_level, \
                    trigger_source"
        if return_col:
            select_option = return_col
        query = f"SELECT {select_option} FROM " + \
                "senslopedb.internal_alert_symbols ias " + \
            "INNER JOIN " + \
                "operational_trigger_symbols ots USING (trigger_sym_id) " + \
            "INNER JOIN " + \
                "trigger_hierarchies th USING (source_id)"

        if trigger_type:
            query = f"{query} WHERE BINARY ias.alert_symbol = '{trigger_type}'"
        else:
            if trigger_symbol:
                query = f"{query} WHERE BINARY ots.alert_symbol = '{trigger_symbol}'"

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        return_data = None
        if return_col:
            if result:
                return_data = result[0][0]
        else:
            if result:
                result = result[0]
                return_data = {
                    "internal_sym_id": result[0], 
                    "trigger_sym_id": result[1], 
                    "ias_symbol": result[2], 
                    "ots_symbol": result[3], 
                    "alert_description": result[4], 
                    "alert_level": result[5], 
                    "trigger_source": result[6]
                }

        return return_data


    def get_operational_trigger_symbol(trigger_source, alert_level, return_col=None):
        """
        Returns tuple operational_trigger row

        Args:
            trigger_source (str) - 
            alert_level (int) - 
        """
        select_option = "ots.*"
        if return_col:
            select_option = return_col
        query = f"SELECT {select_option} FROM operational_trigger_symbols as ots "
        query += "INNER JOIN trigger_hierarchies as th USING (source_id) "
        query += f"WHERE th.trigger_source = '{trigger_source}' "
        query += f"AND ots.alert_level = {alert_level}"

        schema = "senslopedb"
        result = DB.db_read(query, schema)
        
        return_data = result[0]
        if return_col:
            return_data = result[0][0]

        return return_data

    def get_trigger_hierarchy(source_id, return_col=None):
        select_option = "*"
        if return_col:
            select_option = return_col

        query = f"SELECT {select_option} FROM trigger_hierarchies"
        query = f"{query} WHERE source_id = {source_id}"
        schema = "senslopedb"
        result = DB.db_read(query, schema)

        return_data = result[0]
        if return_col:
            return_data = result[0][0]

        return return_data
