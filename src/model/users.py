from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
import hashlib

class Users():

	def create_user(username):
		query = ('INSERT INTO users VALUES (0, "%s", "","", 1)') % username
		user_id = DB.db_modify(query,'commons_db', True)
		return user_id

	def create_user_complete_details(firstname, lastname, nickname):
		query = (f"INSERT INTO users VALUES (0, '{firstname}', '{lastname}', '{nickname}', 1)")
		user_id = DB.db_modify(query,'commons_db', True)
		return user_id

	def create_user_account(data):
		try:
			lastname, firstname, nickname, \
			mobile_number, birthdate, username, \
			password, email, site_id, \
			site_code, is_complete_signup = data.values()
		except ValueError:
			is_complete_signup = False
			birthdate = None
			email = None
			site_id = data["site_id"]
			site_code = data["site_code"]
			username = data["username"]
			password = data["password"]
			mobile_number = data["mobile_number"]

		salt =  str(hashlib.md5(str(dt.today()).encode("utf-8")).hexdigest())
		password = str(hashlib.sha512(str(password+salt).encode("utf-8")).hexdigest())

		if "is_complete_signup" in data and data["is_complete_signup"]:
			user_id = Users.create_user_complete_details(firstname, lastname, nickname)
		else:
			user_id = Users.create_user(username)

		profile_id = Users.create_user_profile({
			"birthdate": birthdate,
			"email": email, 
			"site_code": site_code,
			"site_id": site_id,
			"user_id": user_id
		})

		gsm_id, final_number = Users.get_gsm_id(mobile_number)
		mobile_id = Users.create_user_mobile({
			"gsm_id": gsm_id,
			"mobile_status": 1,
			"priority": 1,
			"sim_num": final_number,
			"user_id": user_id,
		})

		query = (f"INSERT INTO user_account VALUES (0, {user_id}, 10, '{username}', '{password}', '{salt}')")
		account_id = DB.db_modify(query,'commons_db', True)
		if account_id != None:
			status = True
		else:
			status = False
		return status

	def get_gsm_id(sim_num):
		"""
		Checks sim_num if Smart Globe
		"""
		num_length = len(sim_num)
		gsm_id = 0
		prefix = "000"
		if num_length > 0:
			if num_length == 11:
				prefix = sim_num[1:4]
				remaining_number = sim_num[2:11]
			elif num_length == 10:
				prefix = sim_num[0:3]
				remaining_number = sim_num[1:10]
			elif num_length == 12:
				prefix = sim_num[2:5]
				remaining_number = sim_num[3:12]
			else:
				print(Exception("Invalid mobile number length"))

		query = f"SELECT gsm_id FROM sim_prefixes WHERE prefix = {prefix}"
		gsm_id = DB.db_read(query, 'comms_db')[0][0]

		return gsm_id, f"639{remaining_number}"

	def create_user_mobile(data):
		gsm_id, mobile_status, priority, sim_num, user_id = data.values()
		query = f"INSERT INTO user_mobile VALUES (0, {user_id}, {sim_num}, {priority}, {mobile_status}, {gsm_id})"
		mobile_id = DB.db_modify(query,'comms_db', True)
		return mobile_id
	
	def create_user_profile(user_prof_data):
		birthdate, email, site_code, site_id, user_id = user_prof_data.values()
		query = f"INSERT INTO user_profile VALUES (0, '{user_id}','{site_id}', '{birthdate}', '{email}')"
		profile_id = DB.db_modify(query,'commons_db', True)
		return profile_id

	def account_exists(username):
		query = ('SELECT COUNT(*) FROM user_account WHERE username = "%s"') % username
		count = DB.db_read(query, 'commons_db')
		return count

	def fetch_account(username):
		query = ('SELECT user_account.user_id, account_id, ' \
		'privilege_id, profile_id, username, password, salt, first_name, ' \
		'last_name, site_id, site_code FROM user_account INNER JOIN user_profile USING (user_id) ' \
		'INNER JOIN users using (user_id) INNER JOIN sites using(site_id) WHERE username = "%s"') % username
		account = DB.db_read(query, 'commons_db')
		return account

	def fetch_user(user_id):
		query = f"SELECT first_name, last_name FROM commons_db.users WHERE user_id = {user_id}"
		user = DB.db_read(query, 'commons_db')
		return user
