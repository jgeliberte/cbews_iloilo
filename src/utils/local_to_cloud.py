import MySQLdb
import subprocess
import sys

class LocalToCloud():

    def last_cloud_inbox_id(self):
        temp_ssh = "ssh dynaslope@dynaslope.phivolcs.dost.gov.ph "
        temp_mysql = "\"mysql -ucbewsl -pcb3wsls3rv3r -e'use comms_db; select ts_sms from smsinbox_loggers order by ts_sms desc limit 1'\""
        print(temp_ssh+temp_mysql)
        proc = subprocess.Popen([temp_ssh+temp_mysql], stdout=subprocess.PIPE, shell=True)
        (out, err) = proc.communicate()
        temp_last_smslogger_id = out.decode("utf-8").split("\n")
        if temp_last_smslogger_id[0] == '':
            temp_last_smslogger_id = 0
        else:
            temp_last_smslogger_id = temp_last_smslogger_id[1]
        return temp_last_smslogger_id

    def fetch_last_cloud_rain_data(self):
        print("1")

    def fetch_last_cloud_surficial_data(self):
        print("2")

    def fetch_last_cloud_subsurface_data(self):
        print("3")

    def fetch_last_cloud_moms_data(self):
        print("4")

    def fetch_last_cloud_eq_data(self):
        print("5")

    def rack_connect(self, ip, user, password):
        try:
            #Static for now
            db = MySQLdb.connect(ip, user, password, 'comms_db')
            cur = db.cursor()
            return db, cur
        except TypeError as err:
            print('Error Connection Value')
            return False
        except MySQLdb.OperationalError as err:
            print("MySQL Operationial Error:", err)
            return False
        except (MySQLdb.Error, MySQLdb.Warning) as err:
            print("MySQL Error:", err)
            return False

    def fetch_latest_local_data(self, inbox_id):
        try:
            db, cur = self.rack_connect('192.168.150.75', 'pysys_local','NaCAhztBgYZ3HwTkvHwwGVtJn5sVMFgg')
            query = f'SELECT * FROM smsinbox_loggers WHERE ts_sms > "{inbox_id}" LIMIT 100;'
            a = cur.execute(query)
            out = []
            if a:
                out = cur.fetchall()
                db.close()
            return out
        except MySQLdb.OperationalError as err:
            print("MySQLdb OP Error:", err)
            time.sleep(20)

    def sync_local_to_cloud(self, local_data):
        temp_query = ""
        for row in local_data:
            (inbox_id, ts_sms, ts_stored, mobile_id, sms_msg, read_status, web_status, gsm_id) = row
            sms_msg = sms_msg.replace('"','')
            if ts_stored == None:
                ts_stored = ts_sms
            temp_query = temp_query + f'(0, "{str(ts_sms)}", "{str(ts_stored)}", {mobile_id}, "{sms_msg}", 0, {web_status == None and "NULL" or web_status}, {gsm_id}),'
        print(temp_query)
        query = f'INSERT INTO smsinbox_loggers VALUES {temp_query[:-1]};'
        try:
            db, cur = self.rack_connect('202.90.159.64','cbewsl','cb3wsls3rv3r')
            a = cur.execute(query)
            db.commit()
            print(a)
        except MySQLdb.OperationalError as err:
            print("MySQLdb OP Error:", err)
            time.sleep(20)

if __name__ == "__main__":
    Syncer = LocalToCloud()
    while True:
        sms_id = Syncer.last_cloud_inbox_id()
        print("LAST CLOUD ID:", sms_id)
        local_data = Syncer.fetch_latest_local_data(sms_id)
        if (len(local_data) != 0):
            sync_cloud = Syncer.sync_local_to_cloud(local_data)
        else:
            print("No new data. \n\n")