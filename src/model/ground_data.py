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