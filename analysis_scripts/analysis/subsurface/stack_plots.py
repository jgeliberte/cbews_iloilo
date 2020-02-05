"""
end-of-event report plotting tools
"""

##### IMPORTANT matplotlib declarations must always be FIRST to make sure that
##### matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as pltdates
plt.ion()

from datetime import timedelta
import numpy as np
import pandas as pd
#import seaborn

import ColumnPlotter as plotter
import genproc as proc
import querySenslopeDb as qdb
import rtwindow as rtw

mpl.rcParams['xtick.labelsize'] = 8
mpl.rcParams['ytick.labelsize'] = 8

def nonrepeat_colors(ax,NUM_COLORS,color='jet'):
    cm = plt.get_cmap(color)
    ax.set_color_cycle([cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in range(NUM_COLORS)[::-1]])
    return ax

def zeroed(df, column):
    df['zeroed_'+column] = df[column] - df[column].values[0]
    return df

# surficial data
def get_surficial_df(site, start, end):

    query = "SELECT timestamp, site_id, crack_id, meas FROM gndmeas"
    query += " WHERE site_id = '%s'" % site
    query += " AND timestamp <= '%s'"% end
    query += " AND timestamp > '%s'" % start
    query += " ORDER BY timestamp"
    
    df = qdb.GetDBDataFrame(query)    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['crack_id'] = map(lambda x: x.upper(), df['crack_id'])
    
    marker_df = df.groupby('crack_id', as_index=False)
    df = marker_df.apply(zeroed, column='meas')
    
    return df

# surficial plot
def plot_surficial(ax, df, marker_lst):
    if marker_lst == 'all':
        marker_lst = set(df.crack_id)
    ax = nonrepeat_colors(ax,len(marker_lst))
    for marker in marker_lst:
        marker_df = df[df.crack_id == marker]
        ax.plot(marker_df.timestamp, marker_df.zeroed_meas, marker='o',
                label=marker, alpha=1)
    ax.set_ylabel('Displacement\n(cm)', fontsize='small')
    ax.set_title('Surficial Ground Displacement', fontsize='medium')
    ncol = (len(set(df.crack_id)) + 3) / 4
    ax.legend(loc='upper left', ncol=ncol, fontsize='x-small', fancybox = True, framealpha = 0.5)
    ax.grid()

# rainfall data
def get_rain_df(rain_gauge, start, end):
    rain_df = qdb.GetRawRainData(rain_gauge, fromTime=pd.to_datetime(start)-timedelta(3), toTime=end)
    
    rain_df = rain_df[rain_df.rain >= 0]
    rain_df = rain_df.set_index('ts')
    rain_df = rain_df.resample('30min').sum()
    
    rain_df['one'] = rain_df.rain.rolling(window=48, min_periods=1, center=False).sum()
    rain_df['one'] = np.round(rain_df.one, 2)
    rain_df['three'] = rain_df.rain.rolling(window=144, min_periods=1, center=False).sum()
    rain_df['three'] = np.round(rain_df.three, 2)
    
    rain_df = rain_df[(rain_df.index >= start) & (rain_df.index <= end)]
    rain_df = rain_df.reset_index()
    
    return rain_df

# rainfall plot
def plot_rain(ax, df, rain_gauge, plot_inst=True):
    ax.plot(df.ts, df.one, color='green', label='1-day cml', alpha=1)
    ax.plot(df.ts,df.three, color='blue', label='3-day cml', alpha=1)
    
    if max(list(df.one) + list(df.three)) >= 300:
        ax.set_ylim([0, 300])
    
    if plot_inst:
        ax2 = ax.twinx()
        width = float(0.004 * (max(df['ts']) - min(df['ts'])).days)
        ax2.bar(df['ts'].apply(lambda x: pltdates.date2num(x)), df.rain, width=width,alpha=0.05, color='k', label = '30min rainfall')
        ax2.xaxis_date()
    
    query = "SELECT * FROM rain_props where name = '%s'" %site
    twoyrmax = qdb.GetDBDataFrame(query)['max_rain_2year'].values[0]
    halfmax = twoyrmax/2
    
    ax.plot(df.ts, [halfmax]*len(df.ts), color='green', label='half of 2-yr max', alpha=1, linestyle='--')
    ax.plot(df.ts, [twoyrmax]*len(df.ts), color='blue', label='2-yr max', alpha=1, linestyle='--')
    
    ax.set_title("%s Rainfall Data" %rain_gauge.upper(), fontsize='medium')  
    ax.set_ylabel('1D, 3D Rain\n(mm)', fontsize='small')  
    ax.legend(loc='upper left', fontsize='x-small', fancybox = True, framealpha = 0.5)
    #ax.grid()

# subsurface data
def get_tsm_data(tsm_name, start, end, plot_type, node_lst):
    
    col = qdb.GetSensorList(tsm_name)[0]
    
    window, config = rtw.getwindow(pd.to_datetime(end))

    window.start = pd.to_datetime(start)
    window.offsetstart = window.start - timedelta(days=(config.io.num_roll_window_ops*window.numpts-1)/48.)
    
    if plot_type == 'cml':
        config.io.to_smooth = 1
        config.io.to_fill = 1
    else:
        config.io.to_smooth = 1
        config.io.to_fill = 1

    monitoring = proc.genproc(col, window, config, 'bottom', comp_vel=False)
    df = monitoring.disp_vel.reset_index()[['ts', 'id', 'xz', 'xy']]
    df = df.loc[(df.ts >= window.start)&(df.ts <= window.end)]
    df = df.sort_values('ts')
    
    if plot_type == 'cml':
        xzd_plotoffset = 0
        if node_lst != 'all':
            df = df[df.id.isin(node_lst)]
        df = plotter.cum_surf(df, xzd_plotoffset, col.nos)
    else:
        node_df = df.groupby('id', as_index=False)
        df = node_df.apply(zeroed, column='xz')
        df['zeroed_xz'] = df['zeroed_xz'] * 100
        node_df = df.groupby('id', as_index=False)
        df = node_df.apply(zeroed, column='xy')
        df['zeroed_xy'] = df['zeroed_xy'] * 100
    
    return df

# subsurface cumulative displacement plot
def plot_cml(ax, df, axis, tsm_name):
    ax.plot(df.index, df[axis].values)
    ax.set_ylabel('Cumulative\nDisplacement\n(m)', fontsize='small')
    ax.set_title('%s Subsurface Cumulative %s Displacement' %(tsm_name.upper(), axis.upper()), fontsize='medium')
    ax.grid()
    
# subsurface displacemnt plot
def plot_disp(ax, df, axis, node_lst, tsm_name):
    ax = nonrepeat_colors(ax,len(node_lst))
    for node in node_lst:
        node_df = df[df.id == node]
        ax.plot(node_df.ts, node_df['zeroed_'+axis].values, label='Node '+str(node))
    ax.set_ylabel('Displacement\n(cm)', fontsize='small')
    ax.set_title('%s Subsurface %s Displacement' %(tsm_name.upper(), axis.upper()), fontsize='medium')
    ncol = (len(node_lst) + 3) / 4
    ax.legend(loc='upper left', ncol=ncol, fontsize='x-small', fancybox = True, framealpha = 0.5)
    ax.grid()

def plot_single_event(ax, ts, color='red'):
    ax.axvline(pd.to_datetime(ts), color=color, linestyle='--', alpha=1)    
    
def plot_span(ax, start, end, color):
    ax.axvspan(start, end, facecolor=color, alpha=0.25, edgecolor=None,linewidth=0)

def get_surficial_csv(fname,start,end):
    df = pd.read_csv(fname)
    df['ts'] = pd.to_datetime(df.ts)
    df = df.set_index('ts').truncate(start,end).reset_index()
    df['site_code'] = df.site_code.str.upper()
    df['marker_name'] = df.marker_name.str.upper()
    df['marker_name'] = df.marker_name.str.replace('CRACK ','')
    df['marker_name'] = df.marker_name.str.replace('LIKI ', '')
    
#    df = df.dropna(subset=['ts','meas'])
    dfg = df.groupby('marker_name', as_index=False)
    df = dfg.apply(zeroed, column='meas')
    return df

def plot_from_csv(ax, df, marker_lst):    
    if marker_lst == 'all':
        marker_lst = set(df.marker_name)
        
    ax = nonrepeat_colors(ax,len(marker_lst))
    for marker in marker_lst:
        temp = df[df.marker_name == marker]
        ax.plot(temp.ts, temp.zeroed_meas, marker='o',
                label=marker, alpha=1)
    
    ax.set_ylabel('Displacement\n(cm)', fontsize='small')
    ax.set_title('Surficial Ground Displacement', fontsize='medium')
    ncol = (len(set(df.marker_name)) + 3) / 4
    ax.legend(loc='upper left', ncol=ncol, fontsize='x-small', fancybox = True, framealpha = 0.5)
    ax.grid()   

def main(site, start, end, rainfall_props, surficial_props, subsurface_props, csv_props, event_lst, span_list):
    subsurface_end = subsurface_props['end']
    # count of subplots in subsurface displacement
    disp = subsurface_props['disp']['to_plot']
    num_disp = 0
    disp_plot = subsurface_props['disp']['disp_tsm_axis']
    disp_plot_key = disp_plot.keys()
    for i in disp_plot_key:
        num_disp += len(disp_plot[i].keys())
    disp = [disp] * num_disp

    # count of subplots in subsurface displacement
    cml = subsurface_props['cml']['to_plot']
    num_cml = 0
    cml_plot = subsurface_props['cml']['cml_tsm_axis']
    cml_plot_key = cml_plot.keys()
    for i in cml_plot_key:
        num_cml += len(cml_plot[i].keys())
    cml = [cml] * num_cml

    # total number of subplots in subsurface
    subsurface = disp + cml

    # total number of subplots
    num_subplots = ([rainfall_props['to_plot']]*len(rainfall_props['rain_gauge_lst']) +
                 [surficial_props['to_plot']]+ [csv_props['to_plot']] + subsurface).count(True)
    subplot = num_subplots*101+10

    x_size = 8
    y_size = 5*num_subplots
    fig=plt.figure(figsize = (x_size, y_size))

    if rainfall_props['to_plot']:
        for rain_gauge in rainfall_props['rain_gauge_lst']:
            ax = fig.add_subplot(subplot)
            rain = get_rain_df(rain_gauge, start, end)
            if rain_gauge != rainfall_props['rain_gauge_lst'][0]:
                ax = fig.add_subplot(subplot-1, sharex=ax)
                subplot -= 1                    
                ax.xaxis.set_visible(False)
            plot_rain(ax, rain, rain_gauge.upper().replace('RAIN_NOAH_', 'ASTI ARG '), plot_inst=rainfall_props['plot_inst'])
            for event_id in range(len(event_lst[0])):
                try:
                    color = event_lst[1][event_id]
                except:
                    color = 'red'
                plot_single_event(ax, event_lst[0][event_id], color=color)
            
            for startTS, endTS, color in span_list:
                plot_span(ax, startTS, endTS, color)
    
    if surficial_props['to_plot']:
        surficial = get_surficial_df(site, start, end)
        try:
            ax = fig.add_subplot(subplot-1, sharex=ax)
            subplot -= 1
        except:
            ax = fig.add_subplot(subplot)
        if rainfall_props['to_plot']:
            ax.xaxis.set_visible(False)
        plot_surficial(ax, surficial, surficial_props['markers'])
        for event_id in range(len(event_lst[0])):
            try:
                color = event_lst[1][event_id]
            except:
                color = 'red'
            plot_single_event(ax, event_lst[0][event_id], color=color)
            
        for startTS, endTS, color in span_list:
            plot_span(ax, startTS, endTS, color)

    if csv_props['to_plot']:
        df = get_surficial_csv(fname, start, end)
        try:
            ax = fig.add_subplot(subplot-1, sharex=ax)
            subplot -= 1
        except:
            ax = fig.add_subplot(subplot)
        if rainfall_props['to_plot']:
            ax.xaxis.set_visible(False)
        plot_from_csv(ax, df, csv_props['markers'])
        for event_id in range(len(event_lst[0])):
            try:
                color = event_lst[1][event_id]
            except:
                color = 'red'
            plot_single_event(ax, event_lst[0][event_id], color=color)
            
        for startTS, endTS, color in span_list:
            plot_span(ax, startTS, endTS, color)

    if subsurface_props['disp']['to_plot']:
        disp = subsurface_props['disp']['disp_tsm_axis']
        for tsm_name in disp.keys():
            tsm_data = get_tsm_data(tsm_name, start, subsurface_end, 'disp', 'all')
            if subsurface_props['mirror_xz']:
                tsm_data['zeroed_xz'] = -tsm_data['zeroed_xz']
            if subsurface_props['mirror_xy']:
                tsm_data['zeroed_xy'] = -tsm_data['zeroed_xy']

            axis_lst = disp[tsm_name]
            for axis in axis_lst.keys():
                try:
                    ax = fig.add_subplot(subplot-1, sharex=ax)
                    subplot -= 1
                except:
                    ax = fig.add_subplot(subplot)
                ax.xaxis.set_visible(False)
                plot_disp(ax, tsm_data, axis, axis_lst[axis], tsm_name)
                for event_id in range(len(event_lst[0])):
                    try:
                        color = event_lst[1][event_id]
                    except:
                        color = 'red'
                    plot_single_event(ax, event_lst[0][event_id], color=color)
          
                for startTS, endTS, color in span_list:
                    plot_span(ax, startTS, endTS, color)

    if subsurface_props['cml']['to_plot']:
        cml = subsurface_props['cml']['cml_tsm_axis']
        for tsm_name in cml.keys():
            node_lst = []
            for node in cml[tsm_name].values():
                if node != 'all':
                    node_lst += node
                else:
                    node_lst += [node]
            if 'all' in node_lst:
                node_lst = 'all'
            else:
                node_lst = list(set(node_lst))
            tsm_data = get_tsm_data(tsm_name, start, subsurface_end, 'cml', node_lst)
            if subsurface_props['mirror_xz']:
                tsm_data['xz'] = -tsm_data['xz']
            if subsurface_props['mirror_xy']:
                tsm_data['xy'] = -tsm_data['xy']
                
            axis_lst = cml[tsm_name]
            for axis in axis_lst.keys():
                try:
                    ax = fig.add_subplot(subplot-1, sharex=ax)
                    subplot -= 1
                except:
                    ax = fig.add_subplot(subplot)
                ax.xaxis.set_visible(False)
                plot_cml(ax, tsm_data, axis, tsm_name)
                for event_id in range(len(event_lst[0])):
                    try:
                        color = event_lst[1][event_id]
                    except:
                        color = 'red'
                    plot_single_event(ax, event_lst[0][event_id], color=color)
                
                for startTS, endTS, color in span_list:
                    plot_span(ax, startTS, endTS, color)

    ax.set_xlim([start, end])
    fig.subplots_adjust(top=0.9, right=0.95, left=0.15, bottom=0.05, hspace=0.3)
    fig.suptitle(site.upper() + " Event Timeline",fontsize='x-large')
    plt.savefig(site + "_event_timeline", dpi=200,mode='w')#, 
#        facecolor='w', edgecolor='w',orientation='landscape')

############################################################

if __name__ == '__main__':
    
    site = 'tue'
    start = '2018-07-06'
    end = '2018-08-02'
    subsurface_end = '2018-08-02'
    
    # annotate events
    event_lst = ['2018-07-23 15:30' ]#['2017-12-15 05:16', '2017-12-16 12:30', '2017-12-16 18:00']
    event_color = ['orange']#['gold', 'red', 'red'] # [] kapag red lang everything
    
    span_starts = []#['2017-10-14 09:30', '2017-10-18 16:00', '2017-10-28 16:00', '2017-10-28 17:35'] 
    span_ends = []#['2017-10-17 16:00', '2017-10-21 20:00', '2017-10-28 17:35', '2017-11-04 20:00']
    alert = []
    span_colors = ['red'] #['yellow', 'yellow', 'yellow', 'red']
    span_list = zip(span_starts, span_ends, span_colors)
    
    
    # rainfall plot                                                 
    rainfall = True                             ### True if to plot rainfall
    plot_inst = True                           ### True if to plot instantaneous rainfall
    rain_gauge_lst = ['rain_noah_469']                ### specifiy rain gauge
    rainfall_props = {'to_plot': rainfall, 'rain_gauge_lst': rain_gauge_lst, 'plot_inst': plot_inst}

    # surficial plot
    surficial = False             ### True if to plot surficial
    markers = ['D','J','L','M','N']    ### specifiy markers; 'all' if all markers
    surficial_props = {'to_plot': surficial, 'markers': markers}
    
    
    # from csv
    from_csv = False               ### True if to plot surficial
    fname = 'nag_surficialdata.csv'
    markers = ['D','J','L','M','N']    ### specifiy markers; 'all' if all markers
    csv_props = {'to_plot': from_csv, 'markers': markers, 'fname':fname}
    
    # subsurface plot
    
    mirror_xz = False
    mirror_xy = False
    
    # subsurface displacement
    disp = True                 ### True if to plot subsurface displacement
    ### specifiy tsm name and axis; 'all' if all nodes
    disp_tsm_axis = {'Tueta': {'xz': [1,9,10]}, 'tuetb': {'xz': [5,6,7,8,9,10]}} #{'xz': [7,8,9], 'xy': [7,8,9]},
            #'blcsb': {'xy': range(1,11), 'xz': range(1,11)}}
    
    # subsurface cumulative displacement
    cml = False     ### True if to plot subsurface cumulative displacement
    ### specifiy tsm name and axis; 'all' if all nodes
    cml_tsm_axis = {'nagsa': {'xz': [1,9,10]}, 'tuetb': {'xz': [5,6,7,8,9,10]}} 
    #'sagtb': {'xz':range(3,20)}} #'magtb': {'xy': range(11,16), 'xz': range(11,16)}}
    
    subsurface_props = {'disp': {'to_plot': disp, 'disp_tsm_axis': disp_tsm_axis},
                       'cml': {'to_plot': cml, 'cml_tsm_axis': cml_tsm_axis},
                        'end': subsurface_end, 'mirror_xz': mirror_xz,
                        'mirror_xy': mirror_xy}
    
    df = main (site, start, end, rainfall_props, surficial_props, subsurface_props, csv_props, [event_lst, event_color], span_list)
    
################################################################################
    
#    def ts_range(df, lst):
#        lst.append((pd.to_datetime(df['ts'].values[0]), pd.to_datetime(df['ts'].values[0]) + timedelta(hours=0.5)))
#        return lst
#    
#    def stitch_intervals(ranges):
#        result = []
#        cur_start = -1
#        cur_stop = -1
#        for start, stop in sorted(ranges):
#            if start != cur_stop:
#                result.append((start,stop))
#                cur_start, cur_stop = start, stop
#            else:
#                result[-1] = (cur_start,stop)
#                cur_stop = max(cur_stop,stop)
#        return result
#    
#    rain = get_rain_df('pintaw', '2016-03-15', '2017-09-13')
#    query = "SELECT * FROM rain_props where name = '%s'" %'pin'
#    twoyrmax = qdb.GetDBDataFrame(query)['max_rain_2year'].values[0]
#    halfmax = twoyrmax/2
#    
#    halfT = rain[(rain.one >= halfmax/2)|(rain.three >= halfmax)]
#    
#    lst = []
#    halfTts = halfT.groupby('ts', as_index=False)
#    rain_ts_range = halfTts.apply(ts_range, lst=lst)
#    rain_ts_range = rain_ts_range[0]
#    rain_ts_range = rain_ts_range[1::]
#    halfT_range = stitch_intervals(rain_ts_range)