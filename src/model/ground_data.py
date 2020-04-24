from src.model.helper.utils import DatabaseConnection as DB
from datetime import datetime as dt
from src.api.helpers import Helpers

class GroundData():

    def fetch_surficial_data(site_id):
        query = f'SELECT ts, measurement, marker_name, observer_name, weather ' \
                f'FROM senslopedb.site_markers INNER JOIN ' \
                f'commons_db.sites USING (site_id) INNER JOIN ' \
                f'senslopedb.marker_data USING (marker_id) INNER JOIN senslopedb.marker_observations USING (mo_id) ' \
                f'WHERE sites.site_id = "{site_id}" ORDER BY ts desc limit 100;'
        schema = DB.db_switcher(site_id)
        result = DB.db_read(query, schema)
        return result

    def fetch_surficial_plot_data(marker_id, site_code, start, end):
        query = f'SELECT mo_id, data_id, marker_id, ts as x, measurement as y FROM senslopedb.marker_data INNER ' \
                f'JOIN marker_observations USING (mo_id) WHERE (ts BETWEEN "{start}" AND "{end}") and marker_id = {marker_id} ' \
                'order by ts asc;'
        result = DB.db_read(query, 'senslopedb')
        return result

    def fetch_surficial_markers(site_id):
        query = f'SELECT marker_id, marker_name ' \
            f'FROM senslopedb.site_markers INNER JOIN commons_db.sites USING (site_id) ' \
            f'WHERE sites.site_id = "{site_id}" ORDER BY marker_name;'

        schema = DB.db_switcher(site_id)
        result = DB.db_read(query, schema)
        return result


    def update_surficial_marker_values(mo_id, marker_id, value):
        query = f'UPDATE marker_data set measurement="{value}" WHERE marker_id = "{marker_id}" AND mo_id = "{mo_id}";'
        result = DB.db_modify(query, 'senslopedb', True)
        return result
    

    def update_surficial_marker_observation(mo_id, ts, weather, observer, site_id):
        try:
            query = f'UPDATE marker_observations SET ts="{ts}", ' \
                f'observer_name="{observer}", weather="{weather}" ' \
                f'WHERE site_id = "{site_id}" AND mo_id = "{mo_id}";'
            update_status = DB.db_modify(query, 'senslopedb', True)
            result = {"status": True, "data": update_status}
        except Exception as err:
            result = {"status": False, "message": err}
        finally:
            return result


    def insert_marker_observation(data):
        try:
            (ts, weather, observer, marker_value, site_id) = data.values()
            query = f'INSERT INTO marker_observations VALUES (0, "{site_id}", "{ts}", "EVENT", ' \
                f'"{observer}", "APP", 1, "{weather}");'
            mo_id = DB.db_modify(query, 'senslopedb', True)
            result = {"status": True, "mo_id": mo_id}
        except Exception as err:
            result = {"status": False, "message": err}
        finally:
            return result


    def fetch_marker_ids_v_moid(mo_id):
        query = f'SELECT marker_id, marker_name FROM senslopedb.marker_data ' \
            f'INNER JOIN senslopedb.marker_names ON marker_id = name_id where mo_id = "{mo_id}";'
        result = DB.db_read(query, 'senslopedb')
        return result


    def fetch_surficial_mo_id(ts, site_id):
        query = f'SELECT mo_id FROM senslopedb.marker_observations WHERE ts = "{ts}" and site_id = "{site_id}" limit 1;'
        schema = DB.db_switcher(site_id)
        result = DB.db_read(query, schema)
        return result


    def insert_marker_values(id, value, mo_id):
        try:
            query = f'INSERT INTO marker_data VALUES (0, {mo_id}, {id}, {value})'
            mo_id = DB.db_modify(query, 'senslopedb', True)
            result = {"status": True, "data": mo_id}
        except Exception as err:
            result = {"status": False, "message": err}
        finally:
            return result


    def delete_marker_observation(surficial_data):
        try:
            if 'mo_id' in surficial_data:
                (site_id, mo_id) = surficial_data.values()
                query = f'DELETE FROM marker_observations WHERE mo_id="{mo_id}" AND site_id="{site_id}'
            else:
                (ts, weather, observer, site_id) = surficial_data.values()
                query = f'DELETE FROM marker_observations WHERE ts="{ts}" ' \
                        f'AND weather="{weather}" AND observer_name="{observer}" AND site_id = "{site_id}" ' \
                        'AND IFNULL(mo_id, 0) = LAST_INSERT_ID(mo_id);'
            mo_status = DB.db_modify(query, 'senslopedb', True)
            if mo_status is None:
                result = {"status": True, "mo_id": mo_id}
            else:
                result = {"status": True, "mo_id": mo_status}
        except Exception as err:
            print(err)
            result = {"status": False, "mo_id": None}
        finally:
            return result


    def delete_marker_values(mo_id):
        try:
            query = f'DELETE FROM marker_data WHERE mo_id = "{mo_id}"'
            status = DB.db_modify(query, 'senslopedb', True)
            result = {"status": True, "message": "Successfully delete surficial data."}
        except Exception as err:
            result = {"status": False, "message": "Failed to delete surficial data."}      
        finally:
            return result


    def fetch_moms(site_id):
        try:
            query = 'SELECT * FROM senslopedb.moms_instances INNER ' \
                    'JOIN monitoring_moms USING(instance_id) INNER JOIN moms_features USING(feature_id) ' \
                    f'WHERE site_id = "{site_id}" ORDER BY observance_ts DESC;'
            result = DB.db_read(query, 'senslopedb')
        except Exception as err:
            result = {"status": False, "message": "Failed to delete surficial data."} 
        finally:
            return result


    def insert_moms_instance(site_id, feature_id, feature_name, 
                            location, reporter):
        try:
            query = f'INSERT INTO moms_instances VALUES (0, {site_id}, {feature_id}, "{feature_name}", ' \
                    f'"{location}", "{reporter}")'
            status = DB.db_modify(query, 'senslopedb', True)
            result = status
        except Exception as err:
            result = {"status": False, "message": "Failed to add MoMs data."}      
        finally:
            return result


    def insert_moms_record(instance, ts, remarks,
                            reporter_id, alert_level=0, validator_id=None):
        """
        Inserts monitoring_moms row

        Args:
            instance (int) - instance_id of the feature to be reported
            ts (str/datetime) - observance_ts of the moms report
            remarks (str) - remarks of the moms report
            reporter_id (int) - in CBEWSL, one reporter and
                                validator is enough - user_id
            alert_level (int) - op_trigger of the moms report
            validator_id (int) - in CBEWSL, one reporter and
                                validator is enough - user_id
        """
        try:
            if isinstance(ts, str):
                ts = Helpers.str_to_dt(ts)
            if not validator_id:
                validator = reporter_id
            query = "INSERT INTO monitoring_moms "
            query += "(instance_id, observance_ts, reporter_id, remarks, validator, op_trigger)"
            query += f"VALUES ({instance}, '{ts}', {reporter_id}, '{remarks}', {validator}, {alert_level})"

            status = DB.db_modify(query, 'senslopedb', True)
            result = {"status": True, "data": status}
        except Exception as err:
            result = {"status": False, "message": f"Failed to add MoMs Record data. Error: {err}"}      
        finally:
            return result


    def fetch_feature_name(feature_id, feature, site_id):
        try:
            if isinstance(feature, str) and feature != '':
                query = f'SELECT instance_id, feature_name, location, reporter FROM moms_instances WHERE feature_name = "{feature}" AND ' \
                    f'site_id = {site_id} AND feature_id = {feature_id}'
            else:
                query = f'SELECT instance_id, feature_name, location, reporter FROM moms_instances WHERE ' \
                    f'site_id = {site_id} AND feature_id = {feature_id}'
            result = DB.db_read(query, 'senslopedb')
        except Exception as err:
            result = {"status": False, "message": "Failed to retrieve MoMs data."} 
        finally:
            return result
    
    def update_moms_instance(instance_id, location, reporter):
        try:
            query = f'UPDATE moms_instances SET location = "{location}", reporter = "{reporter}" WHERE instance_id = {instance_id}'
            update_status = DB.db_modify(query, 'senslopedb', True)
            result = {"status": True, "data": update_status}
        except Exception as err:
            result = {"status": False, "message": err}
        finally:
            return result

    def update_monitoring_moms(moms_id, remarks):
        try:
            query = f'UPDATE monitoring_moms SET remarks = "{remarks}" WHERE moms_id = {moms_id}'
            update_status = DB.db_modify(query, 'senslopedb', True)
            result = {"status": True, "data": update_status}
        except Exception as err:
            result = {"status": False, "message": err}
        finally:
            return result

    def delete_moms_observation(moms_id):
        try:
            query = f'DELETE FROM monitoring_moms WHERE moms_id = {moms_id}'
            update_status = DB.db_modify(query, 'senslopedb', True)
            result = {"status": True, "data": update_status}
        except Exception as err:
            result = {"status": False, "message": err}
        finally:
            return result


    def get_latest_od_events(site_id):
        query = "SELECT id, ts, site_id, reason, reporter, alert_level FROM public_alert_on_demand " 
        query += f"WHERE site_id = {site_id} " 
        query += "ORDER BY ts DESC"
        print(query)

        od_data = DB.db_read(query, 'senslopedb')

        return_list = []
        if od_data:
            return_list = []
            for row in od_data:
                return_list.append({
                    "id": row[0],
                    "ts": Helpers.dt_to_str(row[1]),
                    "site_id": row[2],
                    "reason": row[3],
                    "reporter": row[4],
                    "alert_level": row[5]
                })

        return return_list


    def insert_on_demand_alert(ts, site_id, reason, reporter, alert_level):
        """
        """
        try:
            query = "INSERT INTO public_alert_on_demand (ts, site_id, reason, reporter, alert_level) " \
                    f"VALUES ('{ts}', {site_id}, '{reason}', '{reporter}', {alert_level})"
            od_id = DB.db_modify(query, "senslopedb", last_insert_id=True)
            result = { "status": True, "data": od_id }
        except Exception as err:
            print(err)
            result = { "status": False, "data": None }

        return result
