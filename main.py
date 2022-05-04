from operator import truediv
import sqlalchemy
import csv
from sqlalchemy import create_engine, MetaData, Integer, String, Table, Column, Float

meta = MetaData()


measures = Table(
   'measures', meta,
   Column('station', String, unique=False),
   Column('date', String),
   Column('precip', Float),
   Column('tobs', Integer),
)

stations = Table(
   'stations', meta,
   Column('station', String, primary_key=True),
   Column('latitude', Float),
   Column('longitude', Float),
   Column('elevation', Float), 
   Column('name', String),
   Column('country', String),
   Column('state', String),
)

def import_measures_from_csv():
    with open('clean_measure-1.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row1 in reader:
            conn.execute(ins_measures, [{'station':row1['station'], 'date':row1['date'], 'precip':row1['precip'], 'tobs':row1['tobs']}])

def import_stations_from_csv():
    with open('clean_stations-1.csv', newline='') as csvfile:
        reader2 = csv.DictReader(csvfile)
        for row1 in reader2:
            conn.execute(ins_stations, [{'station':row1['station'], 'latitude':row1['latitude'], 'longitude':row1['longitude'], 'elevation':row1['elevation'], 'name':row1['name'], 'country':row1['country'], 'state':row1['state']}])

if __name__== "__main__":

    engine = create_engine('sqlite:///database.db', echo=True)

    meta.create_all(engine)

    ins_measures = measures.insert()
    ins_stations = stations.insert()

    conn = engine.connect()
    

    if not "database.db":
        import_measures_from_csv()
        import_stations_from_csv()


    result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    # result = conn.execute(measures.select().where(measures.c.precip > 10))

    for row in result:
        print(row)

    

    