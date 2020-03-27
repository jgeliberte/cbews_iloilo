from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class Earthquake():
	def get_latest_eq_events(limit = 100):
		query = f"SELECT * FROM earthquake_events ORDER BY ts LIMIT {limit}"
		eq_data = DB.db_read(query, 'senslopedb')
		return eq_data
