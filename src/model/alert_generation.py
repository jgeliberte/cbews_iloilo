from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class AlertGeneration():

    def get_ongoing_extended_overdue_events(site_id=None, complete=False):
        """
        Returns ongoing, extended, and routine events
        """
        select_option = "site_id, status"
        if complete:
            select_option = "*"

        query = f"SELECT {select_option} FROM public_alert_event \
                WHERE status in ('on-going', 'extended')"
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
        select_option = "release_id, data_timestamp, internal_alert_level, release_time"
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
        select_option = "release_id, trigger_type, timestamp, info"
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
