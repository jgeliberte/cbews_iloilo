from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
from src.api.helpers import Helpers as h

class Maintenance():

    def create_maintenance_log(self, data):
        (maintenance_ts, maintenance_type, remarks, in_charge, updater, site_id) = data.values()
        schema = DB.db_switcher(site_id)
        query = f'INSERT INTO maintenance_logs (maintenance_ts, maintenance_type, remarks, in_charge, updater) ' \
                f'VALUES ("{maintenance_ts}", "{maintenance_type}", "{remarks}", "{in_charge}", "{updater}")'
        maintenance_log_id = DB.db_modify(query, schema, True)
        h.var_checker("maintenance_log_id", maintenance_log_id, True)
        return maintenance_log_id

    def fetch_maintenance_log(self, site_id, maintenance_log_id=None, ts_dict={}):
        """
        Args:
            site_id
            maintenance_log_id (int/None)
            ts_dict (Dictionary) - { start: string, end: string }
        """
        query = 'SELECT * FROM maintenance_logs'
        where_clause = ""
        if maintenance_log_id:
            where_clause = f'maintenance_log_id = {maintenance_log_id}'
        elif ts_dict.items():
            where_clause = f'"{ts_dict["start"]}" <= maintenance_ts AND maintenance_ts <= "{ts_dict["end"]}"'

        if where_clause:
            query = f'{query} WHERE {where_clause}'

        schema = DB.db_switcher(site_id)
        result = DB.db_read(query, schema)
        if result:
            temp = []
            for row in result:
                temp.append({
                    "maintenance_log_id": row[0],
                    "maintenance_ts": h.dt_to_str(row[1]),
                    "maintenance_type": row[2],
                    "remarks": row[3],
                    "in_charge": row[4],
                    "updater": row[5]
                })
            result = temp
        return result
    
    def update_maintenance_log(self, data):
        (maintenance_log_id, maintenance_ts, maintenance_type, remarks, in_charge, updater, site_id) = data.values()
        query = f'UPDATE maintenance_logs SET ' \
            f'maintenance_ts="{maintenance_ts}", maintenance_type="{maintenance_type}", ' \
            f'remarks="{remarks}", in_charge="{in_charge}", updater="{updater}" ' \
            f'WHERE maintenance_log_id="{ maintenance_log_id }"'
        schema = DB.db_switcher(site_id)
        result = DB.db_modify(query, schema, True)
        return result

    def delete_maintenance_log(self, maintenance_log_id, site_id):
        query = f'DELETE FROM maintenance_logs WHERE maintenance_log_id = { maintenance_log_id }'
        schema = DB.db_switcher(site_id)
        result = DB.db_modify(query, schema, True)
        return result
