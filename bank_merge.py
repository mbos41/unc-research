import pandas as pd
import numpy as np
import os

os.chdir('/Users/michaelbostwick/Documents/RA')


def load_merge(date):
    cust_subcalls = pd.read_csv('raw_data/{}_cust_subcalls.txt'.format(date), encoding="utf-8-sig")
    agent_profile = pd.read_csv('raw_data/{}_agent_profile.txt'.format(date), encoding="utf-8-sig")
    agent_records = pd.read_csv('raw_data/{}_agent_records.txt'.format(date), encoding="utf-8-sig")
    agent_events = pd.read_csv('raw_data/{}_agent_events.txt'.format(date), encoding="utf-8-sig")

    agent_records = agent_records.filter(items=['call_id', 'agent', 'record_id', 'work_time', 'wait_time', 'ring_time',
                                                'talk_time', 'hold_time', 'wrapup_time', 'consult_time',
                                                'consult_group',
                                                'consult_service', 'nother_calls'])

    agent_events = agent_events[agent_events.event_id.isin([3, 4, 5, 6, 7])].filter(
        items=['agent', 'event_id', 'duration',
               'event_end'])

    subcalls = cust_subcalls.filter(items=['call_id', 'segment_start', 'party_answered'])
    merge_temp = pd.merge(subcalls, agent_events, left_on='party_answered', right_on='agent', how='left')
    merge_temp_fltr = merge_temp[(merge_temp.segment_start > merge_temp.event_end)]
    events = merge_temp_fltr.groupby(['call_id', 'segment_start', 'party_answered', 'event_id'])['duration'].aggregate(
        ['count', sum]).unstack().fillna(0)

    events.columns = events.columns.droplevel()
    events.columns = ['event3_count', 'event4_count', 'event5_count', 'event6_count', 'event7_count',
                      'event3_duration', 'event4_duration', 'event5_duration', 'event6_duration', 'event7_duration']

    merge1 = pd.merge(cust_subcalls, agent_profile, left_on='party_answered', right_on='agent', how='left',
                      suffixes=["_cust", "_profile"])


    merge2 = pd.merge(merge1, agent_records, left_on=['call_id', 'record_id', 'party_answered'],
                  right_on=['call_id', 'record_id', 'agent'], how='left', suffixes=["_cust", "_records"])

    merge3 = pd.merge(merge2, events, left_on=['call_id', 'segment_start', 'party_answered'], right_index=True, how='left')

    time = pd.to_datetime(merge3.segment_start, unit='s')
    merge3['contact_year'], merge3['contact_month'] = time.year, time.month
    merge3['contact_day'], merge3['contact_hour'] = time.day, time.hour

    return merge3

merged = load_merge('D01012008')
merged.to_csv('clean_data/D01012008.csv')