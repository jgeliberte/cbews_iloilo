from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
from src.api.helpers import Helpers

class AlertGeneration():

    def validate_operational_trigger(trigger_id):
        """
        """
        print("test")


    def insert_operational_trigger(site_id, trig_sym_id, ts_updated):
        """Inserts operational_trigger table entry.

        Args:
            site_id (int) - where will the trigger be associated
            trig_sym_id (int) - trigger_sym_id - the kind of trigger
            ts_updated (str/datetime) - since this is an insert, we can assume
                            the ts and ts_updated is the same.
        """
        if isinstance(ts_updated, str):
            ts_updated = Helpers.str_to_dt(ts_updated)
        ts = ts_updated
        query = "INSERT INTO senslopedb.operational_triggers "
        query += "(ts, site_id, trigger_sym_id, ts_updated) "
        query += f"VALUES ({ts}, {site_id}, {trig_sym_id}, {ts_updated})"

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
        query = "UPDATE senslopedb.operational_triggers "
        query += f"SET trigger_sym_id={trig_sym_id}, trigger_sym_id={trig_sym_id}, ts_updated={ts_updated})"
        query += f"WHERE trigger_id = {op_trig_id}"

        schema = "senslopedb"
        result = DB.db_modify(query, schema, True)

        return result


    def fetch_recent_operational_trigger(site_id, trig_sym_id=None):
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

        return result[0]


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

        return result


    def get_event_releases(event_id, sort_order="desc", return_count=None, complete=False):
        """
        Returns public_alert_releases row/s
        """
        select_option = "release_id, data_timestamp, internal_alert_level, release_time, reporter_id_mt"
        if complete:
            select_option = "*"

        query = f"SELECT {select_option} FROM public_alert_release \
                INNER JOIN public_alert_event using (event_id)"
        
        order = "ASC" if sort_order in ["asc", "ASC"] else "DESC"
        query = f"{query} ORDER BY release_id {order}"

        query = f"{query} LIMIT {return_count}" if return_count else query

        # schema = DB.db_switcher(site_id)
        schema = "senslopedb"
        result = DB.db_read(query, schema)

        return result


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

        return result


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
        H = Helpers
        select_option = "*"
        if return_col:
            select_option = return_col
        query = f"SELECT {select_option} FROM public_alert_symbols WHERE "

        # Either you give level or symbol. Pretty obvious one.
        if alert_level:
            query = f"{query} alert_level = {alert_level}"
        elif alert_symbol:
            query = f"{query} alert_symbol = {alert_symbol}"

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        if return_col:
            result = result[0][0]

        return result


    def get_internal_alert_symbol_row(trigger_type=None, trigger_symbol=None, return_col=None):
        """
        Util miniquery
        """
        H = Helpers
        select_option = f"internal_sym_id, \
                    ias.trigger_sym_id, \
                    ias.alert_symbol as ots_symbol, \
                    ots.alert_symbol as alert_symbol, \
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
            query = f"{query} WHERE ias.alert_symbol = '{trigger_type}'"
        else:
            if trigger_symbol:
                query = f"{query} WHERE ots.alert_symbol = '{trigger_symbol}'"

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        if return_col:
            result = result[0][0]

        return result


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

        return result[0]


    def get_trigger_hierarchy(source_id, return_col=None):
        H = Helpers
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
        
