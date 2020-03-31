from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class Earthquake():
	def get_latest_eq_events(limit = 50):
		query = "SELECT eq_id, ts, magnitude,depth,latitude, longitude, distance, CONCAT(IFNULL(CONCAT(purok, ', '),''), " \
				"IFNULL(CONCAT(sitio,', '), '') ,IFNULL(CONCAT(barangay,', '), '') ,IFNULL(CONCAT(municipality,', '), ''), " \
				"IFNULL(province, '')) as site FROM earthquake_events INNER JOIN earthquake_alerts USING (eq_id) " \
				f"INNER JOIN commons_db.sites USING(site_id) ORDER BY ts LIMIT  {limit}"
		eq_data = DB.db_read(query, 'senslopedb')
		return eq_data