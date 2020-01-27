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