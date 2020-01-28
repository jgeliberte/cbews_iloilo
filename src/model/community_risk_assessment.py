from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt

class CommunityRiskAssessment():

    def create_cav(data):
        (datetime, resource, quantity, status, owner, incharge, updater, user_id, site_id) = data.values()
        schema = DB.db_switcher(site_id)
        query = f'INSERT INTO capacity_and_vulnerability VALUES (0, "{resource}",{quantity}, "{status}", ' \
                f'"{owner}", "{incharge}", "{updater}", "{datetime}", "{user_id}")'
        cav_id = DB.db_modify(query, schema, True)
        return cav_id

    def fetch_cav(site_id, cav_id):
        if cav_id == "all":
            query = 'SELECT * FROM capacity_and_vulnerability'
        else:
            query = f'SELECT * FROM capacity_and_vulnerability WHERE cav_id = "{cav_id}"'
        schema = DB.db_switcher(site_id)
        result = DB.db_read(query, schema)
        return result
    
    def update_cav(data):
        (cav_id ,datetime ,resource ,quantity ,status ,owner ,incharge ,updater ,user_id ,site_id) = data.values()
        query = f'UPDATE capacity_and_vulnerability SET ' \
            f'resource="{ resource }", quantity="{ quantity }", stat_desc="{ status }", ' \
            f'owner="{ owner }", in_charge="{ incharge }", updater="{ updater }", ' \
            f'date="{ datetime }" WHERE cav_id="{ cav_id }"'
        schema = DB.db_switcher(site_id)
        result = DB.db_modify(query, schema, True)
        return result

    def delete_cav(cav_id, site_id):
        query = f'DELETE FROM capacity_and_vulnerability WHERE cav_id = { cav_id }'
        schema = DB.db_switcher(site_id)
        result = DB.db_modify(query, schema, True)
        return result