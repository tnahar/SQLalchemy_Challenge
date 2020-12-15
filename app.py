# Import Dependencies
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import json
import flask
from flask import Flask, jsonify
#%%
# Create an engine for the chinook.sqlite database
engine = create_engine('sqlite:///Resources/hawaii.sqlite')
# %%
app = Flask(__name__)
#%%
@app.route("/")
def welcome():
    return (
        f"Welcome to the Hawaii Temps API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"
    )
# %%
@app.route('/api/v1.0/precipitation')
def prcp():
    conn = engine.connect()
    query = '''
        SELECT
            date,
            AVG(prcp) as avg_prcp
        FROM
            measurement
        WHERE
            date >= (SELECT DATE(MAX(date), '-1 year') FROM measurement)
        GROUP BY
            date
        ORDER BY
            date
'''
    prcp_df = pd.read_sql(query, conn)
    prcp_df['date'] = pd.to_datetime(prcp_df['date'])
    prcp_df.sort_values('date')
#    prcp_df.set_index('date', inplace = True)
    prcp_json = prcp_df.to_json(orient = 'records', date_format = 'iso')
    conn.close()
    return prcp_json
#%%
# /api/v1.0/stations
# Return a JSON list of stations from the dataset.
@app.route('/api/v1.0/stations')
def stn():
    conn = engine.connect()
    query = '''
        SELECT
            s.station AS station_code,
            s.name AS station_name
        FROM
            measurement m
        INNER JOIN station s
        ON m.station = s.station
        GROUP BY
            s.station,
            s.name
    '''
    active_stations_df = pd.read_sql(query, conn)
    active_stations_json = active_stations_df.to_json(orient = 'records')
    conn.close()
    return active_stations_json
#%%
# /api/v1.0/tobs
# Query the dates and temperature observations of the most active station # for the last year of data.
# Return a JSON list of temperature observations (TOBS) for the previous # year.
@app.route('/api/v1.0/tobs')
def mas():
    conn = engine.connect()
    query = '''
        SELECT
            s.station AS station_code,
            s.name AS station_name,
            COUNT(*) AS station_count
        FROM
            measurement m
        INNER JOIN station s
        ON m.station = s.station
        GROUP BY
            s.station,
            s.name
        ORDER BY 
            station_count DESC
    '''
    active_stations_df = pd.read_sql(query, conn)
    active_stations_df.sort_values('station_count', ascending=False, inplace=True)
    most_active_station = active_stations_df['station_code'].values[0]
    query = f'''
        SELECT
            tobs
        FROM
            measurement
        WHERE 
            station = '{most_active_station}'
            AND
            date >= (SELECT DATE(MAX(date), '-1 year') FROM measurement)
    '''
    mas_tobs_df = pd.read_sql(query, conn)
    mas_tobs_json = mas_tobs_df.to_json(orient = 'records')
    conn.close()
    return mas_tobs_json
#%%
#/api/v1.0/<start> and /api/v1.0/<start>/<end>
# Return a JSON list of the minimum temperature, the average temperature, and the max
# temperature for a given start or start-end range.
# When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and # equal to the start date.
# When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates 
# between the start and end date inclusive.
@app.route('/api/v1.0/<start>/<end>')
def date_stat_bounded(start, end):
    conn = engine.connect()
    query = f'''
        SELECT
            MIN(tobs) AS TMIN,
            MAX(tobs) AS TMAX,
            AVG(tobs) AS TAVG
        FROM
            measurement
        WHERE 
            date BETWEEN '{start}' AND '{end}'
    '''
    bounded_date_stats_df = pd.read_sql(query, conn)
    bounded_date_stats_json = bounded_date_stats_df.to_json(orient = 'records')
    conn.close()    
    return bounded_date_stats_json
#%%
@app.route('/api/v1.0/<start>')
def date_stat_open(start):
    conn = engine.connect()
    query = f'''
        SELECT
            MIN(tobs) AS TMIN,
            MAX(tobs) AS TMAX,
            AVG(tobs) AS TAVG
        FROM
            measurement
        WHERE 
            date >= '{start}'
    '''
    open_date_stats_df = pd.read_sql(query, conn)
    open_date_stats_json = open_date_stats_df.to_json(orient = 'records')
    conn.close()    
    return open_date_stats_json
#%%
if __name__ == '__main__':
    app.run(debug=True)