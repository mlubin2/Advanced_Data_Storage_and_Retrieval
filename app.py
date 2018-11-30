import datetime as dt
from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

app = Flask(__name__)

engine = create_engine("sqlite:///hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)

Measurement = Base.classes.measurement
Station = Base.classes.station

session = Session(bind=engine)


def convert_to_dict(query_result, label):
    data = []
    for record in query_result:
        data.append({'date': record[0], label: record[1]})
    return data


def get_most_recent_date():
    most_recent_date_query = session.query(Measurement).\
        order_by(Measurement.date.desc()).limit(1)

    for date in most_recent_date_query:
        most_recent_date = date.date

    return dt.datetime.strptime(most_recent_date, "%Y-%m-%d")


@app.route('/api/v1.0/precipitation')
def return_precipitation():
    most_recent_date = get_most_recent_date()
    one_year_ago = most_recent_date - dt.timedelta(days=365)

    recent_prcp_data = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago).\
        order_by(Measurement.date).all()

    return jsonify(convert_to_dict(recent_prcp_data, label='prcp'))


@app.route('/api/v1.0/stations')
def return_station_list():
    station_list = session.query(Measurement.station).distinct()

    return jsonify([station[0] for station in station_list])


@app.route('/api/v1.0/tobs')
def return_tobs():
    most_recent_date = get_most_recent_date()
    one_year_ago = most_recent_date - dt.timedelta(days=365)

    recent_tobs_data = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date >= one_year_ago).\
        order_by(Measurement.date).all()

    return jsonify(convert_to_dict(recent_prcp_data, label='tobs'))


@app.route('/api/v1.0/<start>')
@app.route('/api/v1.0/<start>/<end>')
def return_weather(start, end=None):
    if end is None:
        end = get_most_recent_date()

    weather_extremes = session.query(func.min(Measurement.tobs),
                                     func.avg(Measurement.tobs),
                                     func.max(Measurement.tobs)).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end).all()

    data = []
    for record in weather_extremes:
        data.append({'TMIN': record[0],
                     'TAVG': record[1],
                     'TMAX': record[2]})

    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)