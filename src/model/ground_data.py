from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class GroundData():

    def fetch_surficial_data(site_id):
        query = f'SELECT ts, measurement, marker_name, observer_name, weather ' \
                f'FROM senslopedb.site_markers INNER JOIN ' \
                f'commons_db.sites USING (site_id) INNER JOIN ' \
                f'senslopedb.marker_data USING (marker_id) INNER JOIN senslopedb.marker_observations USING (mo_id) ' \
                f'WHERE sites.site_id = "{site_id}" ORDER BY ts desc limit 100;'
        schema = DB.db_switcher(site_id)
        result = DB.db_read(query, schema)
        return result

    def fetch_surficial_markers(site_id):
        query = f'SELECT marker_name ' \
            f'FROM senslopedb.site_markers INNER JOIN commons_db.sites USING (site_id) ' \
            f'WHERE sites.site_id = "{site_id}" ORDER BY marker_name;'
        schema = DB.db_switcher(site_id)
        result = DB.db_read(query, schema)
        return result

    def update_surficial_marker_values(mo_id, marker_id, value):
        query = f'UPDATE marker_data set measurement="{value}" WHERE marker_id = "{marker_id}" AND mo_id = "{mo_id}";'
        result = DB.db_modify(query, 'senslopedb', True)
        return result
    
    def update_surficial_marker_observation(mo_id, ts, weather, observer, site_id):
        query = f'UPDATE marker_observations SET ts="{ts}", ' \
            f'observer_name="{observer}", weather="{weather}" ' \
            f'WHERE site_id = "{site_id}" AND mo_id = "{mo_id}";'
        result = DB.db_modify(query, 'senslopedb', True)
        return result

    def fetch_marker_ids(mo_id):
        query = f'SELECT marker_id, marker_name FROM senslopedb.marker_data ' \
            f'INNER JOIN senslopedb.marker_names ON marker_id = name_id where mo_id = "{mo_id}";'
        result = DB.db_read(query, 'senslopedb')
        return result

    def fetch_surficial_mo_id(ts, site_id):
        query = f'SELECT mo_id FROM senslopedb.marker_observations WHERE ts = "{ts}" and site_id = "{site_id}" limit 1;'
        schema = DB.db_switcher(site_id)
        result = DB.db_read(query, schema)
        return result