from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
from src.api.helpers import Helpers

class AlertGeneration():

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
        print(query)
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

        print(query)
        # schema = DB.db_switcher("senslopedb")
        schema = "senslopedb"
        result = DB.db_read(query, schema)

        print(result)
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
        
        H.var_checker("get_public_alert_row query", query, True)

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        if return_col:
            result = result[0][0]

        return result


    def get_internal_alert_symbol_row(trigger_type, return_col=None):
        """
        Util miniquery
        """
        H = Helpers
        select_option = f"internal_sym_id, \
                    ias.trigger_sym_id, \
                    ias.alert_symbol, \
                    ots.alert_symbol, \
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

        query = f"{query} WHERE ias.alert_symbol = '{trigger_type}'"
        H.var_checker("get_internal_alert_symbol_row query", query, True)

        schema = "senslopedb"
        result = DB.db_read(query, schema)

        if return_col:
            print(result)
            result = result[0][0]

        return result
