import MySQLdb
import configparser
import time

# class DatabaseCredentials():
# 	def __new__(self, host):
# 		config = configparser.ConfigParser()
# 		config.read('/home/pi/updews-pycodes/gsm/gsmserver_dewsl3/utils/config.cnf')
# 		if host is not None:
# 			config["MASTER_DB_CREDENTIALS"]["host"] = host
# 		return config

class DatabaseConnection():

    def db_connect(schema):
        try:
            #Static for now
            db = MySQLdb.connect('202.90.159.64',
                                    'cbewsl',
                                    'cb3wsls3rv3r', schema)
            cur = db.cursor()
            return db, cur
        except TypeError as err:
            # self.error_logger.store_error_log(self.exception_to_string(err))
            print('Error Connection Value')
            return False
        except MySQLdb.OperationalError as err:
            # self.error_logger.store_error_log(self.exception_to_string(err))
            print("MySQL Operationial Error:", err)
            return False
        except (MySQLdb.Error, MySQLdb.Warning) as err:
            # self.error_logger.store_error_log(self.exception_to_string(err))
            print("MySQL Error:", err)
            return False

    def db_modify(query, schema, last_insert_id=False):
        ret_val = None
        db, cur = DatabaseConnection.db_connect(schema)

        try:
            a = cur.execute(query)
            db.commit()
            if last_insert_id:
                b = cur.execute('select last_insert_id()')
                b = str(cur.fetchone()[0]) 
                ret_val = b
            else:
                ret_val = a
        except Exception as err:
            ret_val = {"status": False,
            "Message": err}
        except IndexError as err:
            print("IndexError on ")
            print(str(inspect.stack()[1][3]))
            # self.error_logger.store_error_log(self.exception_to_string(err))
        except (MySQLdb.Error, MySQLdb.Warning, MySQLdb.OperationalError) as err:
            # self.error_logger.store_error_log(self.exception_to_string(err))
            ret_val = e
            print(">> MySQL error/warning: %s" % e)
            print("Last calls:")
            for i in range(1, 6):
                try:
                    print("%s," % str(inspect.stack()[i][3]),)
                except IndexError:
                    continue
            print("\n")
        finally:
            db.close()
            return ret_val

    def db_read(query, schema):
        try:
            db, cur = DatabaseConnection.db_connect(schema)
            a = cur.execute(query)
            out = []
            if a:
                out = cur.fetchall()
                db.close()
            return out

        except MySQLdb.OperationalError as err:
            # self.error_logger.store_error_log(self.exception_to_string(err))
            print("MySQLdb OP Error:", err)
            time.sleep(20)
    
    def db_switcher(site_id):
        schema = ""
        if int(site_id) == 29:
            schema = "marirong_db"
        else:
            schema = "umingan_db"
        return schema