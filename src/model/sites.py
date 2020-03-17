from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class Sites():

	def get_site_details(filter_value, site_filter="site_id"):
		"""
        Returns row of sites

        Args:
            site_filter (Str): site_id or site_code
            filter_value (Str or Int): int or str
		"""
		query = f"SELECT * FROM sites WHERE {site_filter} = {filter_value}"
		site_details = DB.db_read(query, 'commons_db')[0][0]

		return site_details
