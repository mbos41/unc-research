import pandas as pd
import numpy as np
import os
from datetime import date, timedelta
import time

os.chdir('/Users/michaelbostwick/Documents/RA')

# Pull in shift start/end from previous/next day for overnight shifts
def shift_update(shifts, prev_day, next_day):
    shifts = shifts.filter(['agent', 'shift_id', 'shift_start', 'shift_end', 'overnight'])
    normal = shifts[shifts.overnight==0]
    overnight_prev = shifts[shifts.overnight==1]
    overnight_next = shifts[shifts.overnight==2]
    
    try:
    	agent_shifts_prev = pd.read_csv('raw_data/{}_agent_shifts.txt'.format(prev_day), encoding="utf-8-sig")
    	agent_shifts_prev = agent_shifts_prev.filter(['agent', 'shift_start', 'overnight'])
    	prev_merge = pd.merge(overnight_prev, agent_shifts_prev, on='agent', how='left')
    	prev_merge = prev_merge[prev_merge.overnight_y != 1].sort_values(['agent', 'overnight_y'], ascending=False).drop_duplicates('agent')
    	prev_merge.loc[prev_merge.shift_start_y.isnull(), 'shift_start_y'] = prev_merge.shift_start_x[prev_merge.shift_start_y.isnull()]
    	prev_merge.drop(['shift_start_x','overnight_y'], axis=1, inplace=True)
    	prev_merge.columns = ['agent', 'shift_id', 'shift_end', 'overnight', 'shift_start']
    # If previous agent_shifts file doesn't exist, leave unchanged
    except (OSError):
    	prev_merge = overnight_prev
    
    try:
    	agent_shifts_next = pd.read_csv('raw_data/{}_agent_shifts.txt'.format(next_day), encoding="utf-8-sig")
    	agent_shifts_next = agent_shifts_next.filter(['agent', 'shift_end', 'overnight'])
    	next_merge = pd.merge(overnight_next, agent_shifts_next, on='agent', how='left')
    	next_merge = next_merge[next_merge.overnight_y != 2].sort_values(['agent', 'overnight_y'], ascending=False).drop_duplicates('agent')
    	next_merge.loc[next_merge.shift_end_y.isnull(), 'shift_end_y'] = next_merge.shift_end_x[next_merge.shift_end_y.isnull()]
    	next_merge.drop(['shift_end_x', 'overnight_y'], axis=1, inplace=True)
    	next_merge.columns = ['agent', 'shift_id', 'shift_start', 'overnight', 'shift_end']
    # If next agent_shifts file doesn't exist, leave unchanged
    except (OSError):
    	next_merge = overnight_next
    	
    shifts = pd.concat([normal, prev_merge, next_merge])
    
    return shifts, prev_merge


def load_merge(date, prev_day, next_day):
    # Load csv files
    cust_subcalls = pd.read_csv('raw_data/{}_cust_subcalls.txt'.format(date), encoding="utf-8-sig")
    agent_profile = pd.read_csv('raw_data/{}_agent_profile.txt'.format(date), encoding="utf-8-sig")
    agent_records = pd.read_csv('raw_data/{}_agent_records.txt'.format(date), encoding="utf-8-sig")
    agent_events = pd.read_csv('raw_data/{}_agent_events.txt'.format(date), encoding="utf-8-sig")
    agent_shifts = pd.read_csv('raw_data/{}_agent_shifts.txt'.format(date), encoding="utf-8-sig")

    # Manually add suffix to a few agent_profile columns
    agent_profile.rename(columns={'agent': 'agent_profile', 'talk_time': 'talk_time_profile', 'consult_time': 'consult_time_profile',
                                 'wrapup_time': 'wrapup_time_profile'}, inplace=True)
    
    # Reduce number of columns and manually add suffix to some columns for agent_records
    agent_records = agent_records.filter(items=['agent', 'call_id', 'record_id', 'work_time', 'wait_time', 'ring_time',
											'talk_time', 'hold_time', 'wrapup_time', 'consult_time', 'consult_group', 
                                            'consult_service', 'nother_calls'])
    agent_records.rename(columns={'agent': 'agent_records', 'talk_time': 'talk_time_records', 'consult_time': 'consult_time_records',
                                 'wrapup_time': 'wrapup_time_records'}, inplace=True)
    
    
    shifts, prev_merge = shift_update(agent_shifts, prev_day, next_day)
    
    
    # Get previous day events for agents with overnight shift and add to events dataframe
    events_prev = pd.read_csv('raw_data/D31122007_agent_events.txt', encoding="utf-8-sig")
    events_prev_keep = pd.merge(prev_merge[['agent','shift_start']], events_prev, on='agent', how='inner')
    events_prev_keep = events_prev_keep[events_prev_keep.event_start >
                                    events_prev_keep.shift_start].drop(['shift_start'],axis=1)
    events_full = pd.concat([events_prev_keep, agent_events])
    
    # Only keep relevant agent events
    events_full = events_full[events_full.event_id.isin([3,4,5,6,7])].filter(items=['agent', 'event_id', 'duration',
                                                                                   'event_end'])
    
    # Merge on shift id to events
    events_merge = pd.merge(events_full, shifts[['agent','shift_start','shift_end', 'shift_id']], 
                          on='agent', how='left')
    events_shifts = events_merge[(events_merge.event_end >= events_merge.shift_start) &
                             (events_merge.event_end <= events_merge.shift_end)]
    events_shifts = events_shifts.drop(['shift_start', 'shift_end'], axis=1)
     
    # Merge on shift id to subcalls
    subcalls_merge = pd.merge(cust_subcalls, shifts[['agent','shift_start','shift_end', 'shift_id', 'overnight']], 
                          left_on='party_answered', right_on='agent', how='left')

    
    subcalls_shifts = subcalls_merge[(subcalls_merge.party_answered == 0) | 
                                (subcalls_merge.segment_start + 1000 >= subcalls_merge.shift_start) & (subcalls_merge.segment_start <= subcalls_merge.shift_end)]
                                # To deal with shift start lag, add 1000s of buffer
    subcalls = subcalls_shifts.filter(items=['call_id', 'segment_start', 'party_answered', 'shift_id'])

    # Merge all possible agent/event pairs
    merge_temp = pd.merge(subcalls, events_shifts, left_on=['party_answered', 'shift_id'], 
                          right_on=['agent', 'shift_id'], how='left')
    
    # Only keep events occuring prior to call and aggregate
    merge_temp_fltr = merge_temp[(merge_temp.segment_start > merge_temp.event_end)]
    # Add fake row to make sure columns for all 5 event_id's
    structure = pd.DataFrame({'party_answered': [99999, 99999, 99999, 99999, 99999],
                          'event_id': [3,4,5,6,7],
                          'duration': [0,0,0,0,0],
                          'call_id': [1,1,1,1,1],
                          'segment_start': [1,1,1,1,1]})
    merge_temp_fltr =pd.concat([merge_temp_fltr, structure])
    events = merge_temp_fltr.groupby(['call_id', 'segment_start', 'party_answered', 'event_id'])['duration'].aggregate(['count', sum]).unstack().fillna(0)
    
    # Rename columns
    events.columns = events.columns.droplevel()
    events.columns = ['event3_count', 'event4_count', 'event5_count', 'event6_count', 'event7_count',
                      'event3_duration', 'event4_duration', 'event5_duration', 'event6_duration', 'event7_duration']
    
    # Merge datasets together, one at a time
    merge1 = pd.merge(subcalls_shifts, agent_profile, left_on='party_answered', right_on='agent_profile', how='left', suffixes=["_cust", "_profile"])
        
    merge2 = pd.merge(merge1, agent_records, left_on=['call_id', 'record_id', 'party_answered'], right_on=['call_id', 'record_id', 'agent_records'], how='left', suffixes=["_cust", "_records"])

    merged = pd.merge(merge2, events, left_on=['call_id', 'segment_start', 'party_answered'], right_index=True, how='left')
    
    # Add time columns
    merged['time_since_in'] = merged.segment_start - merged.shift_start
    merged['time_until_out'] = merged.shift_end - merged.segment_start
    merged['time'] = pd.to_datetime(merged.segment_start,unit='s')
    merged['contact_year'], merged['contact_month'] = merged.time.dt.year, merged.time.dt.month
    merged['contact_day'], merged['contact_hour'] = merged.time.dt.day, merged.time.dt.hour
    merged.drop('time', axis=1, inplace=True)
                  
    return merged

# Create list of all unique days in folder
def file_list():
    files = os.listdir('raw_data/')

    files_short = []
    prev = ''
    for file in files:
        current = file[:9]
        if current != prev and current[0] != '.':
            files_short.append(current)
        prev = current
    
    return files_short
 
days = file_list()
num_files = len(days)
prob_files = []

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December']
 
start = time.clock()
# Loop through each day and export merged csv file to appropriate month folder   
for i, day in enumerate(days):
    month = months[int(day[3:5])-1]
    year = day[-4:]
    prev_date = date(int(year), int(day[3:5]), int(day[1:3])) - timedelta(days=1)
    next_date = date(int(year), int(day[3:5]), int(day[1:3])) + timedelta(days=1)
    yesterday = 'D' + str(prev_date.day).rjust(2,'0') + str(prev_date.month).rjust(2,'0') + str(prev_date.year)
    tomorrow = 'D' + str(next_date.day).rjust(2,'0') + str(next_date.month).rjust(2,'0') + str(next_date.year)
    print(day, ' ', i+1, '/', num_files) 
    try:
    	merged = load_merge(day, yesterday, tomorrow) 
    	merged.to_csv('clean_data/{}{}/{}_merged.csv'.format(month,year,day), index = False)
    except (RuntimeError, TypeError, NameError, OSError, ValueError):
    	prob_files.append(day)
   	

stop = time.clock()
print('time elapsed:', (stop - start) // 60, 'mins')
print('Problem files: ', prob_files)
