from datetime import timedelta, time
import pandas as pd
import numpy as np
import math
from sqlalchemy import create_engine

import querySenslopeDbForWebsite as q

def GetResampledData(r, offsetstart, start, end):
    
    ##INPUT:
    ##r; str; site
    ##start; datetime; start of rainfall data
    ##end; datetime; end of rainfall data
    
    ##OUTPUT:
    ##rainfall; dataframe containing start to end of rainfall data resampled to 30min
    
    #raw data from senslope rain gauge
    rainfall = q.GetRawRainData(r, offsetstart)
    rainfall = rainfall.set_index('ts')
    rainfall = rainfall.loc[rainfall['rain']>=0]
    
    try:
        if rainfall.index[-1] <= end-timedelta(1):
            return pd.DataFrame(data=None)
        
        #data resampled to 30mins
        if rainfall.index[-1]<end:
            blankdf=pd.DataFrame({'ts': [end], 'rain': [0]})
            blankdf=blankdf.set_index('ts')
            rainfall=rainfall.append(blankdf)
        rainfall=rainfall.resample('30min', label='right').sum()
        rainfall=rainfall[(rainfall.index>=start)&(rainfall.index<=end)]
        return rainfall
    except:
        return pd.DataFrame(data=None)
        
def GetUnemptyOtherRGdata(col, offsetstart, start, end):
    
    ##INPUT:
    ##r; str; site
    ##offsetstart; datetime; starting point of interval with offset to account for moving window operations
    
    ##OUTPUT:
    ##df; dataframe; rainfall from noah rain gauge    
    
    #gets data from nearest noah/senslope rain gauge
    #moves to next nearest until data is updated
    
    for n in range(3):            
        r = col[n]
        
        OtherRGdata = GetResampledData(r, offsetstart, start, end)
        if len(OtherRGdata) != 0:
            latest_ts = pd.to_datetime(OtherRGdata.index.values[-1])
            if end - latest_ts < timedelta(1):
                return OtherRGdata, r
    return pd.DataFrame(data = None), r

def onethree_val_writer(rainfall, end):

    ##INPUT:
    ##one; dataframe; one-day cumulative rainfall
    ##three; dataframe; three-day cumulative rainfall

    ##OUTPUT:
    ##one, three; float; cumulative sum for one day and three days

    #getting the rolling sum for the last24 hours
    one = rainfall[(rainfall.index > end - timedelta(1)) & (rainfall.index <= end)]['rain'].sum()
    three = rainfall[(rainfall.index > end - timedelta(3)) & (rainfall.index <= end)]['rain'].sum()
    
    return one,three
        
def summary_writer(r,datasource,twoyrmax,halfmax,rainfall,end,write_alert):

    ##DESCRIPTION:
    ##inserts data to summary

    ##INPUT:
    ##s; float; index    
    ##r; string; site code
    ##datasource; string; source of data: ASTI1-3, SENSLOPE Rain Gauge
    ##twoyrmax; float; 2-yr max rainfall, threshold for three day cumulative rainfall
    ##halfmax; float; half of 2-yr max rainfall, threshold for one day cumulative rainfall
    ##summary; dataframe; contains site codes with its corresponding one and three days cumulative sum, data source, alert level and advisory
    ##alert; array; alert summary container, r0 sites at alert[0], r1a sites at alert[1], r1b sites at alert[2],  nd sites at alert[3]
    ##alert_df;array of tuples; alert summary container; format: (site,alert)
    ##one; dataframe; one-day cumulative rainfall
    ##three; dataframe; three-day cumulative rainfall        
    
    one,three = onethree_val_writer(rainfall, end)

    #threshold is reached
    if one>=halfmax or three>=twoyrmax:
        ralert='r1'
        advisory='Start/Continue monitoring'
    #no data
    elif one==None or math.isnan(one):
        ralert='nd'
        advisory='---'
    #rainfall below threshold
    else:
        ralert='r0'
        advisory='---'

    if write_alert or ralert == 'r1':
        engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
        if ralert == 'r0':
            if one >= halfmax*0.75:
                df = pd.DataFrame({'ts': [end], 'site_id': [r], 'rain_source': [datasource], 'rain_alert': ['rxa'], 'cumulative': [one], 'threshold': [round(halfmax,2)]})
                try:
                    df.to_sql(name = 'rain_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
                except:
                    pass
            if three >= twoyrmax*0.75:
                df = pd.DataFrame({'ts': [end], 'site_id': [r], 'rain_source': [datasource], 'rain_alert': ['rxb'], 'cumulative': [three], 'threshold': [round(twoyrmax,2)]})
                try:
                    df.to_sql(name = 'rain_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
                except:
                    pass
        else:
            if one >= halfmax:
                df = pd.DataFrame({'ts': [end], 'site_id': [r], 'rain_source': [datasource], 'rain_alert': ['r1a'], 'cumulative': [one], 'threshold': [round(halfmax,2)]})
                try:
                    df.to_sql(name = 'rain_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
                except:
                    pass
            if three>=twoyrmax:
                df = pd.DataFrame({'ts': [end], 'site_id': [r], 'rain_source': [datasource], 'rain_alert': ['r1b'], 'cumulative': [three], 'threshold': [round(twoyrmax,2)]})
                try:
                    df.to_sql(name = 'rain_alerts', con = engine, if_exists = 'append', schema = q.Namedb, index = False)        
                except:
                    pass


    summary = pd.DataFrame({'site': [r], '1D cml': [one], 'half of 2yr max': [round(halfmax,2)], '3D cml': [three], '2yr max': [round(twoyrmax,2)], 'DataSource': [datasource], 'alert': [ralert], 'advisory': [advisory]})
    
    return summary

def RainfallAlert(siterainprops, end, s):

    ##INPUT:
    ##siterainprops; DataFrameGroupBy; contains rain noah ids of noah rain gauge near the site, one and three-day rainfall threshold
    
    ##OUTPUT:
    ##evaluates rainfall alert
    
    #rainfall properties from siterainprops
    name = siterainprops['name'].values[0]
    twoyrmax = siterainprops['max_rain_2year'].values[0]
    halfmax=twoyrmax/2
    
    rain_arq = siterainprops['rain_arq'].values[0]
    rain_senslope = siterainprops['rain_senslope'].values[0]
    RG1 = siterainprops['RG1'].values[0]
    RG2 = siterainprops['RG2'].values[0]
    RG3 = siterainprops['RG3'].values[0]
    
    start = end - timedelta(s.io.roll_window_length)
    offsetstart = start - timedelta(hours=0.5)

    try:
        query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'public' AND alert != 'A0' ORDER BY timestamp DESC LIMIT 3" %name
        prev_PAlert = q.GetDBDataFrame(query)
        
        query = "SELECT * FROM senslopedb.site_level_alert WHERE site = '%s' AND source = 'public' AND updateTS >= '%s' ORDER BY timestamp DESC LIMIT 1" %(name, end - timedelta(hours=0.5))
        currAlert = q.GetDBDataFrame(query)['alert'].values[0]

        if currAlert != 'A0':
    
            # one prev alert
            if len(prev_PAlert) == 1:
                start_monitor = pd.to_datetime(prev_PAlert.timestamp.values[0])
            # two prev alert
            elif len(prev_PAlert) == 2:
                # one event with two prev alert
                if pd.to_datetime(prev_PAlert['timestamp'].values[0]) - pd.to_datetime(prev_PAlert['updateTS'].values[1]) <= timedelta(hours=0.5):
                    start_monitor = pd.to_datetime(prev_PAlert['timestamp'].values[1])
                else:
                    start_monitor = pd.to_datetime(prev_PAlert['timestamp'].values[0])
            # three prev alert
            else:
                if pd.to_datetime(prev_PAlert['timestamp'].values[0]) - pd.to_datetime(prev_PAlert['updateTS'].values[1]) <= timedelta(hours=0.5):
                    # one event with three prev alert
                    if pd.to_datetime(prev_PAlert['timestamp'].values[1]) - pd.to_datetime(prev_PAlert['updateTS'].values[2]) <= timedelta(hours=0.5):
                        start_monitor = pd.to_datetime(prev_PAlert.timestamp.values[2])
                    # one event with two prev alert
                    else:
                        start_monitor = pd.to_datetime(prev_PAlert['timestamp'].values[1])
                else:
                    start_monitor = pd.to_datetime(prev_PAlert['timestamp'].values[0])
                    
            query =  "SELECT * FROM site_level_alert "
            query += "WHERE site = '%s' " %name
            query += "and alert not regexp '0|nd' "
            query += "and source != 'public' "
            query += "and timestamp >= '%s' " %start_monitor
            query += "and timestamp <= '%s'" %end
            triggers = q.GetDBDataFrame(query)

            validity = pd.to_datetime(max(triggers['updateTS'].values)) + timedelta(1)
            if currAlert == 'A3':
                validity += timedelta(1)

            if end + timedelta(hours=0.5) >= validity:
                write_alert = True
            else:
                write_alert = False
    
        else:
            write_alert = False

    except:
        write_alert = False
    
    try:
        if rain_arq == None:
            rain_timecheck = pd.DataFrame()
        else:
            #resampled data from senslope rain gauge
            rainfall = GetResampledData(rain_arq, offsetstart, start, end)
            #data not more than a day from end
            rain_timecheck = rainfall[(rainfall.index>=end-timedelta(days=1))]
        
        #if data from rain_arq is not updated, data is gathered from rain_senslope
        if len(rain_timecheck.dropna())<1:
            #from rain_senslope, plots and alerts are processed
            rainfall = GetResampledData(rain_senslope, offsetstart, start, end)
            datasource = rain_senslope
            summary = summary_writer(name,datasource,twoyrmax,halfmax,rainfall,end,write_alert)
                    
        else:
            #alerts are processed if senslope rain gauge data is updated
            datasource = rain_arq
            summary = summary_writer(name,datasource,twoyrmax,halfmax,rainfall,end,write_alert)

    except:
        try:
            #if no data from senslope rain gauge, data is gathered from nearest senslope rain gauge from other site or noah rain gauge
            col = [RG1, RG2, RG3]
            rainfall, r = GetUnemptyOtherRGdata(col, offsetstart, start, end)
            datasource = r
            summary = summary_writer(name,datasource,twoyrmax,halfmax,rainfall,end,write_alert)
        except:
            #if no data for all rain gauge
            rainfall = pd.DataFrame({'ts': [end], 'rain': [np.nan]})
            rainfall = rainfall.set_index('ts')
            datasource="No Alert! No ASTI/SENSLOPE Data"
            summary = summary_writer(name,datasource,twoyrmax,halfmax,rainfall,end,write_alert)
            
    return summary

def alert_toDB(df, end):
    
    query = "SELECT * FROM site_level_alert WHERE site = '%s' AND source = 'rain' AND timestamp = '%s' ORDER BY updateTS DESC LIMIT 1" %(df.site.values[0], end)
    df2 = q.GetDBDataFrame(query)

    if len(df2) == 0:

        query = "SELECT * FROM site_level_alert WHERE site = '%s' AND source = 'rain' AND updateTS <= '%s' ORDER BY updateTS DESC LIMIT 1" %(df.site.values[0], end)        
        df2 = q.GetDBDataFrame(query)

        if len(df2) == 0 or df2.alert.values[0] != df.alert.values[0]:
            df['updateTS'] = end
            engine = create_engine('mysql://'+q.Userdb+':'+q.Passdb+'@'+q.Hostdb+':3306/'+q.Namedb)
            df.to_sql(name = 'site_level_alert', con = engine, if_exists = 'append', schema = q.Namedb, index = False)
        elif df2.alert.values[0] == df.alert.values[0] and pd.to_datetime(df2.updateTS.values[0]) < pd.to_datetime(df.timestamp.values[0]):
            db, cur = q.SenslopeDBConnect(q.Namedb)
            query = "UPDATE senslopedb.site_level_alert SET updateTS='%s' WHERE site = '%s' and source = 'rain' and alert = '%s' and timestamp = '%s'" %(end, df2.site.values[0], df2.alert.values[0], pd.to_datetime(str(df2.timestamp.values[0])))
            cur.execute(query)
            db.commit()
            db.close()

################################     MAIN     ################################

def main(siterainprops, end, s, db_write=True):

    ### Processes Rainfall Alert ###
    summary = RainfallAlert(siterainprops, end, s)
    
    if db_write:
        dbsummary = summary
        dbsummary['timestamp'] = str(end)
        dbsummary['source'] = 'rain'
        dbsummary = dbsummary[['timestamp', 'site', 'source', 'alert']]
        alert_toDB(dbsummary, end)
    
    return summary
