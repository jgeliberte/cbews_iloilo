from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class TemplateCreator():
	def fetch(ewi_id = 0):
		#0 == all
		if ewi_id == 0:
			query = "SELECT * FROM ewi_template;"
		else:
			query = f"SELECT * FROM ewi_template where ewi_id = '{ewi_id}';"
		ewi_data = DB.db_read(query, 'ewi_db')
		return ewi_data

	def add(ewi_data):
		(ewi_id, tag, template, modification_by) = ewi_data.values()
		ts_modified = dt.today()
		query =  f"INSERT INTO ewi_template VALUES (0, '{tag}', '{template}', '{ts_modified}', '{modification_by}');"
		ewi_data = DB.db_modify(query, 'ewi_db', True)
		return ewi_data

	def update(ewi_data):
		(ewi_id, tag, template, modification_by) = ewi_data.values()
		ts_modified = dt.today()
		query = f"UPDATE ewi_template SET template = '{template}', ts_modified='{ts_modified}', modification_by='{modification_by}' WHERE ewi_id = {ewi_id} and tag = '{tag}';"
		ewi_data = DB.db_modify(query, 'ewi_db', True)
		return ewi_data

	def delete(ewi_id):
		query = f"DELETE FROM ewi_template where ewi_id = '{ewi_id}';"
		ewi_data = DB.db_modify(query, 'ewi_db', True)
		return ewi_data