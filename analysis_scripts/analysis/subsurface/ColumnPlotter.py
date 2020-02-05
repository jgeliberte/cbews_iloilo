##### IMPORTANT matplotlib declarations must always be FIRST to make sure that matplotlib works with cron-based automation
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as md
plt.ion()

from datetime import timedelta
import numpy as np
import os
import pandas as pd
from scipy.stats import spearmanr

import filepath

def col_pos(colpos_dfts):  
    colpos_dfts = colpos_dfts.drop_duplicates()
    cumsum_df = colpos_dfts[['xz','xy']].cumsum()
    colpos_dfts['cs_xz'] = cumsum_df.xz.values
    colpos_dfts['cs_xy'] = cumsum_df.xy.values
    return np.round(colpos_dfts, 4)

def compute_depth(colpos_dfts):
    colpos_dfts = colpos_dfts.drop_duplicates()
    cumsum_df = colpos_dfts[['depth']].cumsum()
    cumsum_df['depth'] = cumsum_df['depth'] - min(cumsum_df.depth)
    colpos_dfts['depth'] = cumsum_df['depth'].values
    return np.round(colpos_dfts, 4)

def adjust_depth(colpos_dfts, max_depth):
    depth = max_depth - max(colpos_dfts['depth'].values)
    colpos_dfts['depth'] = colpos_dfts['depth'] + depth
    return colpos_dfts

def compute_colpos(window, config, monitoring_vel, num_nodes, seg_len, fixpoint=''):
    if fixpoint == '':
        column_fix = config.io.column_fix
    else:
        column_fix = fixpoint
    colposdates = pd.date_range(end=window.end, freq=config.io.col_pos_interval, periods=config.io.num_col_pos, name='ts', closed=None)

    mask = monitoring_vel['ts'].isin(colposdates)
    colpos_df = monitoring_vel[mask][['ts', 'id', 'depth', 'xz', 'xy']]
    
    if column_fix == 'top':
        colpos_df0 = pd.DataFrame({'ts': colposdates, 'id': [0]*len(colposdates), 'xz': [0]*len(colposdates), 'xy': [0]*len(colposdates), 'depth': [0]*len(colposdates)})
    elif column_fix == 'bottom':
        colpos_df0 = pd.DataFrame({'ts': colposdates, 'id': [num_nodes+1]*len(colposdates), 'xz': [0]*len(colposdates), 'xy': [0]*len(colposdates), 'depth': [seg_len]*len(colposdates)})
    
    colpos_df = colpos_df.append(colpos_df0, ignore_index = True)
    
    if column_fix == 'top':
        colpos_df = colpos_df.sort_values('id', ascending = True)
    elif column_fix == 'bottom':
        colpos_df = colpos_df.sort_values('id', ascending = False)
    
    colpos_dfts = colpos_df.groupby('ts', as_index=False)
    colposdf = colpos_dfts.apply(col_pos)
    
    colposdf = colposdf.sort_values('id', ascending = True)
    colpos_dfts = colposdf.groupby('ts', as_index=False)
    colposdf = colpos_dfts.apply(compute_depth)
    
    if column_fix == 'bottom':
        max_depth = max(colposdf['depth'].values)
        colposdfts = colposdf.groupby('ts', as_index=False)
        colposdf = colposdfts.apply(adjust_depth, max_depth=max_depth)
    
    colposdf['depth'] = colposdf['depth'].apply(lambda x: -x)
    
    return colposdf

def nonrepeat_colors(ax,NUM_COLORS,color='gist_rainbow'):
    cm = plt.get_cmap(color)
    ax.set_color_cycle([cm(1.*(NUM_COLORS-i-1)/NUM_COLORS) for i in range(NUM_COLORS)[::-1]])
    return ax
    
    
def subplot_colpos(dfts, ax_xz, ax_xy, show_part_legend, config, colposTS):
    i = colposTS.loc[colposTS.ts == dfts.ts.values[0]]['index'].values[0]

    #current column position x
    curcolpos_x = dfts['depth'].values

    #current column position xz
    curax = ax_xz
    curcolpos_xz = dfts['cs_xz'].apply(lambda x: x*1000).values
    curax.plot(curcolpos_xz,curcolpos_x,'.-')
    curax.set_xlabel('horizontal displacement, \n downslope(mm)')
    curax.set_ylabel('depth, m')
    
    #current column position xy
    curax=ax_xy
    curcolpos_xy = dfts['cs_xy'].apply(lambda x: x*1000).values
    if show_part_legend == False:
        curax.plot(curcolpos_xy,curcolpos_x,'.-', label=pd.to_datetime(dfts.ts.values[0]).strftime('%Y-%m-%d %H:%M'))
    else:
        if i % show_part_legend == 0 or i == config.io.num_col_pos - 1:
            curax.plot(curcolpos_xy,curcolpos_x,'.-', label=pd.to_datetime(dfts.ts.values[0]).strftime('%Y-%m-%d'))
        else:
            curax.plot(curcolpos_xy,curcolpos_x,'.-')
    curax.set_xlabel('horizontal displacement, \n across slope(mm)')
    
    
def plot_column_positions(df,colname,end, show_part_legend, config, num_nodes=0, max_min_cml=''):
#==============================================================================
# 
#     DESCRIPTION
#     returns plot of xz and xy absolute displacements of each node
# 
#     INPUT
#     colname; array; list of sites
#     x; dataframe; vertical displacements
#     xz; dataframe; horizontal linear displacements along the planes defined by xa-za
#     xy; dataframe; horizontal linear displacements along the planes defined by xa-ya
#==============================================================================

    try:
        fig=plt.figure()
        ax_xz=fig.add_subplot(121)
        ax_xy=fig.add_subplot(122,sharex=ax_xz,sharey=ax_xz)
    
        ax_xz=nonrepeat_colors(ax_xz,len(set(df.ts.values)),color='plasma')
        ax_xy=nonrepeat_colors(ax_xy,len(set(df.ts.values)),color='plasma')
    
        colposTS = pd.DataFrame({'ts': sorted(set(df.ts)), 'index': range(len(set(df.ts)))})
		
        dfts = df.groupby('ts', as_index=False)
        dfts.apply(subplot_colpos, ax_xz=ax_xz, ax_xy=ax_xy, show_part_legend=show_part_legend, config=config, colposTS=colposTS)
    
#        try:
#            max_min_cml = max_min_cml.apply(lambda x: x*1000)
#            xl = df.loc[(df.ts == end)&(df.id <= num_nodes)&(df.id >= 1)]['depth'].values[::-1]
#            ax_xz.fill_betweenx(xl, max_min_cml['xz_maxlist'].values, max_min_cml['xz_minlist'].values, where=max_min_cml['xz_maxlist'].values >= max_min_cml['xz_minlist'].values, facecolor='0.7',linewidth=0)
#            ax_xy.fill_betweenx(xl, max_min_cml['xy_maxlist'].values, max_min_cml['xy_minlist'].values, where=max_min_cml['xy_maxlist'].values >= max_min_cml['xy_minlist'].values, facecolor='0.7',linewidth=0)
#        except:
#            print 'error in plotting noise env'
    
        for tick in ax_xz.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
            
        for tick in ax_xy.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
       
        for tick in ax_xz.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
            
        for tick in ax_xy.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(10)
    
        plt.subplots_adjust(top=0.92, bottom=0.22, left=0.12, right=0.77)        
        plt.suptitle(colname,fontsize='medium')
        ax_xz.grid(True)
        ax_xy.grid(True)

    except:        
        print colname, "ERROR in plotting column position"
    return ax_xz,ax_xy

def linear_vel(df, num_nodes):
    df[df.columns[0]] = num_nodes - df.columns[0] + 1
    return df
    
def vel_classify(df, config, num_nodes, linearvel=True):
    if linearvel:
        vel = pd.DataFrame(columns=range(1, num_nodes+1), index=sorted(set(df.ts)))
        vel.index.name = 'ts'
        nodal_df = vel.groupby(vel.columns, axis=1)
        velplot = nodal_df.apply(linear_vel, num_nodes=num_nodes)
    else:
        velplot = ''
    df = df.set_index(['ts', 'id'])
    L2mask = (df.abs()>config.io.t_vell2)&(df.abs()<=config.io.t_vell3)
    L3mask = (df.abs()>config.io.t_vell3)
    L2mask = L2mask.reset_index().replace(False, np.nan)
    L3mask = L3mask.reset_index().replace(False, np.nan)
    L2mask = L2mask.dropna()[['ts', 'id']]
    L3mask = L3mask.dropna()[['ts', 'id']]
    return velplot,L2mask,L3mask
    
def noise_envelope(df, tsdf):
    df['ts'] = tsdf
    return df

def plotoffset(df, disp_offset = 'mean'):
    #setting up zeroing and offseting parameters
    nodal_df = df.groupby('id', as_index=False)

    if disp_offset == 'max':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).max()
    elif disp_offset == 'mean':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).mean()
    elif disp_offset == 'min':
        xzd_plotoffset = nodal_df['xz'].apply(lambda x: x.max() - x.min()).min()
    else:
        xzd_plotoffset = 0
    
    return xzd_plotoffset

def cum_surf(df, xzd_plotoffset, num_nodes):
    # defining cumulative (surface) displacement
    dfts = df.groupby('ts')
    cs_df = dfts[['xz', 'xy']].sum()    
    cs_df = cs_df - cs_df.values[0] + xzd_plotoffset * num_nodes
    cs_df = cs_df.sort_index()

    return cs_df

def noise_env(df, max_min_df, window, num_nodes, xzd_plotoffset):
    #creating noise envelope
    first_row = df.loc[df.ts == window.start].sort_values('id').set_index('id')[['xz', 'xy']]
        
    max_min_df['xz_maxlist'] = max_min_df['xz_maxlist'].values - first_row['xz'].values
    max_min_df['xz_minlist'] = max_min_df['xz_minlist'].values - first_row['xz'].values
    max_min_df['xy_maxlist'] = max_min_df['xy_maxlist'].values - first_row['xy'].values
    max_min_df['xy_minlist'] = max_min_df['xy_minlist'].values - first_row['xy'].values
        
    max_min_df = max_min_df.reset_index()
    max_min_df = max_min_df.append([max_min_df] * (len(set(df.ts))-1), ignore_index = True)
    nodal_max_min_df = max_min_df.groupby('id', as_index=False)

    noise_df = nodal_max_min_df.apply(noise_envelope, tsdf = sorted(set(df.ts)))
    nodal_noise_df = noise_df.groupby('id', as_index=False)
    noise_df = nodal_noise_df.apply(df_add_offset_col, offset = xzd_plotoffset, num_nodes = num_nodes)
    noise_df = noise_df.set_index('ts')

    # conpensates double offset of node 1 due to df.apply
    a = noise_df.loc[noise_df.id == 1] - (num_nodes - 1) * xzd_plotoffset
    a['id'] = 1
    noise_df = noise_df.loc[noise_df.id != 1]
    noise_df = noise_df.append(a)
    noise_df = noise_df.sort_index()

    return noise_df

def disp0off(df, window, config, xzd_plotoffset, num_nodes, fixpoint=''):
    if fixpoint == '':
        column_fix = config.io.column_fix
    else:
        column_fix = fixpoint
    if column_fix == 'top':
        df['xz'] = df['xz'].apply(lambda x: -x)
        df['xy'] = df['xy'].apply(lambda x: -x)
    nodal_df = df.groupby('id', as_index=False)
    df0 = nodal_df.apply(df_zero_initial_row, window = window)
    nodal_df0 = df0.groupby('id', as_index=False)
    df0off = nodal_df0.apply(df_add_offset_col, offset = xzd_plotoffset, num_nodes = num_nodes)
    df0off = df0off.set_index('ts')
    
    # conpensates double offset of node 1 due to df.apply
    a = df0off.loc[df0off.id == 1] - (num_nodes - 1) * xzd_plotoffset
    a['id'] = 1
    df0off = df0off.loc[df0off.id != 1]
    df0off = df0off.append(a)
    df0off = df0off.sort_index()

    return df0off

def check_increasing(df, inc_df):
    sum_index = inc_df.loc[inc_df.id == df['id'].values[0]].index[0]
    sp, pval = spearmanr(range(len(df)), df['xz'].values)
    if sp > 0.5:
        inc_xz = int(10 * (round(abs(sp), 1) - 0.5))
    else:
        inc_xz = 0
    sp, pval = spearmanr(range(len(df)), df['xy'].values)
    if sp > 0.5:
        inc_xy = int(10 * (round(abs(sp), 1) - 0.5))
    else:
        inc_xy = 0
    diff_xz = max(df['xz'].values) - min(df['xz'].values)
    diff_xy = max(df['xy'].values) - min(df['xy'].values)
    inc_df.loc[sum_index] = [df['id'].values[0], inc_xz, inc_xy, diff_xz, diff_xy]

def metadata(inc_df):
    node_id = str(int(inc_df['id'].values[0]))

    if inc_df['diff_xz'].values[0]>0.01:
        if inc_df['inc_xz'].values[0]>3:
            text_xz = node_id + '++++'
            xz_text_size = 'large'
        elif inc_df['inc_xz'].values[0]>2:
            text_xz = node_id + '+++'
            xz_text_size = 'large'
        elif inc_df['inc_xz'].values[0]>1:
            text_xz = node_id + '++'
            xz_text_size = 'medium'
        elif inc_df['inc_xz'].values[0]>0:
            text_xz = node_id + '+'
            xz_text_size = 'medium'
        else:
            text_xz = node_id
            xz_text_size = 'x-small'
    else:
        text_xz = node_id
        xz_text_size = 'x-small'
    
    if inc_df['diff_xy'].values[0]>0.01:
        if inc_df['inc_xy'].values[0]>3:
            text_xy = node_id + '++++'
            xy_text_size = 'large'
        elif inc_df['inc_xy'].values[0]>2:
            text_xy = node_id + '+++'
            xy_text_size = 'large'
        elif inc_df['inc_xy'].values[0]>1:
            text_xy = node_id + '++'
            xy_text_size = 'medium'
        elif inc_df['inc_xy'].values[0]>0:
            text_xy = node_id + '+'
            xy_text_size = 'medium'
        else:
            text_xy = node_id
            xy_text_size = 'x-small'
    else:
        text_xy = node_id
        xy_text_size = 'x-small'
    
    inc_df['text_xz'] = text_xz
    inc_df['xz_text_size'] = xz_text_size
    inc_df['text_xy'] = text_xy
    inc_df['xy_text_size'] = xy_text_size

    return inc_df

def node_annotation(monitoring_vel, num_nodes):
    check_inc_df = monitoring_vel.sort_values('ts')
    
    inc_df = pd.DataFrame({'id': range(1, num_nodes+1), 'inc_xz': [np.nan]*num_nodes, 'inc_xy': [np.nan]*num_nodes, 'diff_xz': [np.nan]*num_nodes, 'diff_xy': [np.nan]*num_nodes})
    inc_df = inc_df[['id', 'inc_xz', 'inc_xy', 'diff_xz', 'diff_xy']]
    nodal_monitoring_vel = check_inc_df.groupby('id', as_index=False)
    nodal_monitoring_vel.apply(check_increasing, inc_df=inc_df)
    
    nodal_inc_df = inc_df.groupby('id', as_index=False)
    inc_df = nodal_inc_df.apply(metadata)

    return inc_df

def plot_disp_vel(noise_df, df0off, cs_df, colname, window, config, plotvel,
                  xzd_plotoffset, num_nodes, velplot, plot_inc, inc_df=''):
#==============================================================================
# 
#     DESCRIPTION:
#     returns plot of xz and xy displacements per node, xz and xy velocities per node
# 
#     INPUT:
#     xz; array of floats; horizontal linear displacements along the planes defined by xa-za
#     xy; array of floats; horizontal linear displacements along the planes defined by xa-ya
#     xz_vel; array of floats; velocity along the planes defined by xa-za
#     xy_vel; array of floats; velocity along the planes defined by xa-ya
#==============================================================================

    if plotvel:
        vel_xz, vel_xy, L2_xz, L2_xy, L3_xz, L3_xy = velplot

    nodal_noise_df = noise_df.groupby('id', as_index=False)
    
    nodal_df0off = df0off.groupby('id', as_index=False)
    
    fig=plt.figure()

    try:
        if plotvel:
            #creating subplots        
            ax_xzd=fig.add_subplot(141)
            ax_xyd=fig.add_subplot(142,sharex=ax_xzd,sharey=ax_xzd)
            ax_xzd.grid(True)
            ax_xyd.grid(True)
            
            ax_xzv=fig.add_subplot(143)
            ax_xzv.invert_yaxis()
            ax_xyv=fig.add_subplot(144,sharex=ax_xzv,sharey=ax_xzv)
        else:
            #creating subplots        
            ax_xzd=fig.add_subplot(121)
            ax_xyd=fig.add_subplot(122,sharex=ax_xzd,sharey=ax_xzd)
            ax_xzd.grid(True)
            ax_xyd.grid(True)
    except:
        if plotvel:
            #creating subplots                      
            ax_xzv=fig.add_subplot(121)
            ax_xzv.invert_yaxis()
            ax_xyv=fig.add_subplot(122,sharex=ax_xzv,sharey=ax_xzv)

    try:
        dfmt = md.DateFormatter('%Y-%m-%d\n%H:%M')
        ax_xzd.xaxis.set_major_formatter(dfmt)
        ax_xyd.xaxis.set_major_formatter(dfmt)
    except:
        print 'Error in setting date format of x-label in disp subplots'

    #plotting cumulative (surface) displacments
    ts = cs_df.reset_index()['ts'].apply(lambda x: mpl.dates.date2num(x)).values
    for axis in ['xz', 'xy']:
        if axis == 'xz':
            curax = ax_xzd
        else:
            curax = ax_xyd

        plt.sca(curax)
        plt.plot_date(ts, cs_df[axis].values, color='0.4', marker=None,
                      linestyle='-', linewidth=0.5)
        plt.fill_between(ts, cs_df[axis].values, xzd_plotoffset*(num_nodes),
                         color='0.8')

    #assigning non-repeating colors to subplots axis
    ax_xzd=nonrepeat_colors(ax_xzd,num_nodes)
    ax_xyd=nonrepeat_colors(ax_xyd,num_nodes)
    
    if plotvel:
        ax_xzv=nonrepeat_colors(ax_xzv,num_nodes)
        ax_xyv=nonrepeat_colors(ax_xyv,num_nodes)

    #plotting displacement for xz and xy
    for axis in ['xz', 'xy']:
        if axis == 'xz':
            curax = ax_xzd
            title = 'downslope'
        else:
            curax = ax_xyd
            title = 'across slope'

        plt.sca(curax)
        nodal_df0off[axis].apply(plt.plot)
        try:
            nodal_noise_df[axis + '_maxlist'].apply(plt.plot, ls=':')
            nodal_noise_df[axis + '_minlist'].apply(plt.plot, ls=':')
        except:
            print 'Error in plotting noise envelope'
        curax.set_title('displacement\n ' + title,fontsize='small')
        curax.set_ylabel('displacement scale, m', fontsize='small')
        y = df0off.loc[df0off.index == window.start].sort_values('id')[axis].values
        x = window.start
        z = range(1, num_nodes+1)
        if not plot_inc:
            for i,j in zip(y,z):
                curax.annotate(str(int(j)),xy=(x,i),xytext = (5,-2.5),
                               textcoords='offset points', size = 'x-small')
        else:
            for i,j in zip(y,z):
                text = inc_df.loc[inc_df.id == j]['text_' + axis].values[0]
                text_size = inc_df.loc[inc_df.id == j][axis + '_text_size'].values[0]
                curax.annotate(text,xy=(x,i),xytext = (5,-2.5),
                               textcoords='offset points', size = text_size )

    if plotvel:
        #plotting velocity for xz and xy
        for axis in ['xz', 'xy']:
            if axis == 'xz':
                curax = ax_xzv
                vel = vel_xz
                L2 = L2_xz
                L3 = L3_xz
                title = 'downslope'
            else:
                curax = ax_xyv
                vel = vel_xy
                L2 = L2_xy
                L3 = L3_xy
                title = 'across slope'

            vel.plot(ax=curax, marker='.', legend=False)
    
            L2 = L2.sort_values('ts', ascending = True).set_index('ts')
            nodal_L2 = L2.groupby('id', as_index=False)
            nodal_L2.apply(lambda x: x['id'].plot(marker='^', ms=8, mfc='y',
                           lw=0, ax = curax))
    
            L3 = L3.sort_values('ts', ascending = True).set_index('ts')
            nodal_L3 = L3.groupby('id', as_index=False)
            nodal_L3.apply(lambda x: x['id'].plot(marker='^', ms=10, mfc='r',
                           lw=0, ax = curax))
            
            y = sorted(range(1, num_nodes+1), reverse = True)
            x = (vel_xz.index)[1]
            z = sorted(range(1, num_nodes+1), reverse = True)
            for i,j in zip(y,z):
                curax.annotate(str(int(j)), xy=(x,i), xytext = (5,-2.5),
                               textcoords='offset points', size = 'x-small')            
            curax.set_ylabel('node ID', fontsize='small')
            curax.set_title('velocity alerts\n ' + title, fontsize='small')  
            
    # rotating xlabel
    
    for tick in ax_xzd.xaxis.get_minor_ticks():
        tick.label.set_rotation('vertical')
        tick.label.set_fontsize(6)
        
    for tick in ax_xyd.xaxis.get_minor_ticks():
        tick.label.set_rotation('vertical')
        tick.label.set_fontsize(6)
    
    for tick in ax_xzd.xaxis.get_major_ticks():
        tick.label.set_rotation('vertical')
        tick.label.set_fontsize(6)
        
    for tick in ax_xyd.xaxis.get_major_ticks():
        tick.label.set_rotation('vertical')
        tick.label.set_fontsize(6)

    if plotvel:
        for tick in ax_xzv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
    
        for tick in ax_xyv.xaxis.get_major_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
            
        for tick in ax_xzv.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)
    
        for tick in ax_xyv.xaxis.get_minor_ticks():
            tick.label.set_rotation('vertical')
            tick.label.set_fontsize(6)

    for item in ([ax_xzd.xaxis.label, ax_xyd.xaxis.label]):
        item.set_fontsize(8)

    if plotvel:
        for item in ([ax_xyv.yaxis.label, ax_xzv.yaxis.label]):
            item.set_fontsize(8)

    try:
        dfmt = md.DateFormatter('%Y-%m-%d\n%H:%M')
        ax_xzd.xaxis.set_major_formatter(dfmt)
        ax_xyd.xaxis.set_major_formatter(dfmt)
    except:
        print 'Error in setting date format of x-label in disp subplots'

    fig.tight_layout()
    
    fig.subplots_adjust(top=0.85)        
    fig.suptitle(colname, fontsize='medium')
    line = mpl.lines.Line2D((0.5, 0.5), (0.1, 0.8))
    fig.lines = [line]
    
    return


def df_zero_initial_row(df, window):
    #zeroing time series to initial value;
    #essentially, this subtracts the value of the first row
    #from all the rows of the dataframe
    columns = list(df.columns)
    columns.remove('ts')
    columns.remove('id')
    for m in columns:
        df[m] = df[m] - df.loc[df.ts == window.start][m].values[0]
    return np.round(df,4)

def df_add_offset_col(df, offset, num_nodes):
    #adding offset value based on column value (node ID);
    #topmost node (node 1) has largest offset
    columns = list(df.columns)
    columns.remove('ts')
    columns.remove('id')
    for m in columns:
        df[m] = df[m] + (num_nodes - df.id.values[0]) * offset
    return np.round(df,4)
    
    
def main(monitoring, window, config, plotvel_start='', plotvel_end='',
         plotvel=True, show_part_legend = False, realtime=True, plot_inc=True,
         comp_vel=True, end_mon=False, non_event_path=True,
         mirror_xz=False, mirror_xy=False):

    colname = monitoring.colprops.name
    num_nodes = monitoring.colprops.nos
    seg_len = monitoring.colprops.seglen

    if comp_vel == True:
        monitoring_vel = monitoring.disp_vel.reset_index()[['ts', 'id', 'depth', 'xz', 'xy', 'vel_xz', 'vel_xy']]
    else:
        monitoring_vel = monitoring.disp_vel.reset_index()[['ts', 'id', 'depth', 'xz', 'xy']]
    monitoring_vel = monitoring_vel.loc[(monitoring_vel.ts >= window.start)&(monitoring_vel.ts <= window.end)]

    if realtime:
        file_path = {'event': None}
        output_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        file_path['monitoring_output'] = output_path + config.io.outputfilepath+'realtime/'
        if not os.path.exists(file_path['monitoring_output']):
            os.makedirs(file_path['monitoring_output'])
    
    else:
        file_path = filepath.output_file_path(colname[0:3], 'subsurface', monitoring_end=end_mon, \
            positive_trigger=True, end=window.end)

    # noise envelope
    max_min_df = monitoring.max_min_df
    max_min_cml = monitoring.max_min_cml
            
    # compute column position
    colposdf = compute_colpos(window, config, monitoring_vel, num_nodes, seg_len)
    if mirror_xz:
        colposdf['cs_xz'] = -colposdf['cs_xz']
    if mirror_xy:
        colposdf['cs_xy'] = -colposdf['cs_xy']

    # plot column position
    plot_column_positions(colposdf,colname,window.end, show_part_legend, config, num_nodes=num_nodes, max_min_cml=max_min_cml)
    
    lgd = plt.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')

    if realtime:
        config.io.outputfilepath = config.io.outputfilepath+'realtime/'
    
    if non_event_path:
        plt.savefig(file_path['monitoring_output'] + colname + '_ColPos_' + \
                    str(window.end.strftime('%Y-%m-%d_%H-%M')) + '.png',
                    dpi=160, facecolor='w', edgecolor='w',
                    orientation='landscape', mode='w', bbox_extra_artists=(lgd,))

    if file_path['event']:
        plt.savefig(file_path['event'] + colname + '_ColPos_' + \
                str(window.end.strftime('%Y-%m-%d_%H-%M')) + '.png', dpi=160,
                facecolor='w', edgecolor='w', orientation='landscape', mode='w',
                bbox_extra_artists=(lgd,))

    if not realtime:
        plt.close()

    inc_df = node_annotation(monitoring_vel, num_nodes)

    # displacement plot offset
    xzd_plotoffset = plotoffset(monitoring_vel, disp_offset = 'mean')
    
    # defining cumulative (surface) displacement
    cs_df = cum_surf(monitoring_vel, xzd_plotoffset, num_nodes)

    #creating displacement noise envelope
    noise_df = noise_env(monitoring_vel, max_min_df, window, num_nodes, xzd_plotoffset)
    
    #zeroing and offseting xz,xy
    df0off = disp0off(monitoring_vel, window, config, xzd_plotoffset, num_nodes)
    
    if plotvel:
        if plotvel_end == '':
            plotvel_end = window.end
        if plotvel_start == '':
            plotvel_start = plotvel_end - timedelta(hours=3)
        #velplots
        vel = monitoring_vel.loc[(monitoring_vel.ts >= plotvel_start) & (monitoring_vel.ts <= plotvel_end)]
        #vel_xz
        vel_xz = vel[['ts', 'vel_xz', 'id']]
        velplot_xz,L2_xz,L3_xz = vel_classify(vel_xz, config, num_nodes)
        
        #vel_xy
        vel_xy = vel[['ts', 'vel_xy', 'id']]
        velplot_xy,L2_xy,L3_xy = vel_classify(vel_xy, config, num_nodes)
        
        velplot = velplot_xz, velplot_xy, L2_xz, L2_xy, L3_xz, L3_xy
    else:
        velplot = ''
    
    # plot displacement and velocity
    plot_disp_vel(noise_df, df0off, cs_df, colname, window, config, plotvel,
                  xzd_plotoffset, num_nodes, velplot, plot_inc, inc_df=inc_df)
    
    if non_event_path:
        plt.savefig(file_path['monitoring_output'] + colname + '_DispVel_' + \
                    str(window.end.strftime('%Y-%m-%d_%H-%M')) + '.png',
                       dpi=160, facecolor='w', edgecolor='w',
                       orientation='landscape', mode='w')
                
    if file_path['event'] != None:
        plt.savefig(file_path['event'] + colname + '_DispVel_' + \
                str(window.end.strftime('%Y-%m-%d_%H-%M')) + '.png', dpi=160, 
                facecolor='w', edgecolor='w',orientation='landscape',mode='w')

    if not realtime:
        plt.close()