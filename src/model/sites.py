from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class Sites():

	def get_site_details(filter_value, site_filter="site_id", return_col=None):
		"""
        Returns row of sites

        Args:
            site_filter (Str): site_id or site_code
            filter_value (Str or Int): int or str
		"""
		option = "*"
		if return_col:
			option = return_col
		query = f"SELECT {option} FROM sites WHERE {site_filter} = '{filter_value}'"
		print()
		print(query)
		print()
		result = DB.db_read(query, 'commons_db')

		if result: 
			result = result[0]
			if return_col:
				result = result[0][0]
			else:
				result = {
					"site_id":  result[0],
					"site_code":  result[1],
					"purok":  result[2],
					"sitio":  result[3],
					"barangay":  result[4],
					"municipality":  result[5],
					"province":  result[6],
					"region":  result[7],
					"psgc":  result[8],
					"active":  result[9],
					"households":  result[10],
					"season":  result[11]
				}

		return result
