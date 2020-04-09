from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class OnDemand():
	def get_latest_od_events(limit = 50):
		query = "SELECT eq_id, ts, magnitude,depth,latitude, longitude, distance, CONCAT(IFNULL(CONCAT(purok, ', '),''), " \
				"IFNULL(CONCAT(sitio,', '), '') ,IFNULL(CONCAT(barangay,', '), '') ,IFNULL(CONCAT(municipality,', '), ''), " \
				"IFNULL(province, '')) as site FROM earthquake_events INNER JOIN earthquake_alerts USING (eq_id) " \
				f"INNER JOIN commons_db.sites USING(site_id) ORDER BY ts LIMIT  {limit}"
		eq_data = DB.db_read(query, 'senslopedb')
		return eq_data

    def write_on_demand_alert():
        """
        
        """
        query = "INSERT INTO public_alert_ondemand () " \
                f"VALUES ()"
        od_id = DB.db_modify(query, schema, last_insert_id=True)

        return od_id