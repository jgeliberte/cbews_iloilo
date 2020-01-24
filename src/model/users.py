from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
import hashlib

class Users():

	def create_user(username):
		query = ('INSERT INTO users VALUES (0, "%s", "","", 1)') % username
		user_id = DB.db_modify(query,'commons_db', True)
		return user_id

	def create_user_account(data):
		username, password, mobile_number = data.values()
		salt =  str(hashlib.md5(str(dt.today()).encode("utf-8")).hexdigest())
		password = str(hashlib.sha512(str(password+salt).encode("utf-8")).hexdigest())
		user_id = Users.create_user(username)
		query = ('INSERT INTO user_account VALUES (0, %s, 10, "%s", "%s", "%s")') % (user_id, username, password, salt)
		account_id = DB.db_modify(query,'commons_db', True)
		if account_id != None:
			status = True
		else:
			status = False
		return status

	def create_user_mobile():
			print("test")
	
	def account_exists(username):
		query = ('SELECT COUNT(*) FROM user_account WHERE username = "%s"') % username
		count = DB.db_read(query, 'commons_db')
		return count

	def fetch_account(username):
		query = ('SELECT * FROM user_account WHERE username = "%s"') % username
		account = DB.db_read(query, 'commons_db')
		return account
