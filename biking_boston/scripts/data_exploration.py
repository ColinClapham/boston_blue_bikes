import pandas as pd

def read_trips_data():
    trips = pd.read_parquet('../inputs/blue_bikes_master_trip_data.parquet', engine='pyarrow')
    return trips

def read_hub_data():
    hubs = pd.read_csv('../inputs/blue_bikes_hub_data.csv')
    return hubs

def clean_trip_columns():

    trips = read_trips_data()

    ### standard member/non-member status
    trips['member_casual'] = ['member' if member == 'Customer' else ('casual' if member == 'Subscriber' else member) for
                              member in trips['member_casual']]
    ### convert column to timestamps
    trips['started_at'] = pd.to_datetime(trips['started_at'], format='mixed')
    trips['ended_at'] = pd.to_datetime(trips['ended_at'], format='mixed')
