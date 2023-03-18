from flask import Flask
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from werkzeug.exceptions import HTTPException
from dotenv import load_dotenv
import os
import json
from flask_cors import CORS

load_dotenv()

app = Flask(__name__)
CORS(app)


server = os.getenv('ESDAT_SERVER')
database = os.getenv('ESDAT_DATABASE')
esdat_pid = os.getenv('ESDAT_PID')  #PID is EsDAT project identification number

querystring_locs = "SELECT * FROM Locations WHERE PID = " + esdat_pid
querystring_mandips = "SELECT * FROM Groundwater_and_NAPL_Levels WHERE PID = " + esdat_pid
querystring_wells = "SELECT * FROM Wells WHERE PID = " + esdat_pid
querystring_boreholes = "SELECT * FROM Boreholes WHERE PID = " + esdat_pid


@app.route('/')
def index():
    return 'hi matey'

@app.route('/loc/<name>')
def get_info(name):

    try:
        # sqlcon = create_engine('mssql+pyodbc://@' + server + '/' + database + '?driver=SQL+Server')
        sqlcon = create_engine('mssql+pyodbc://@' + server + '/' + database + '?driver=SQL+Server?Trusted_Connection=yes')

        
        with sqlcon.begin() as conn:

            mydict = dict()
            mydict['name'] = name

            ### well details
            df = pd.read_sql(sql=text(querystring_wells),con=conn)
            df = df.loc[df['Location_Code'] == name]
            df = df[['Well', 'TOC', 'Top_Screen_Depth', 'Bottom_Screen_Depth']]
            myjson = df.to_json(orient='records')
            mydict['well'] = json.loads(myjson)

            ### location details
            df = pd.read_sql(sql=text(querystring_locs),con=conn)
            df = df.loc[df['Location_Code'] == name]
            df = df[['x_coord', 'y_coord', 'Elevation']]
            myjson = df.to_json(orient='records')
            mydict['location'] = json.loads(myjson)

            ### borehole details
            df = pd.read_sql(sql=text(querystring_boreholes),con=conn)
            df = df.loc[df['Location_Code'] == name]
            df = df[['Bearing', 'Plunge']]
            myjson = df.to_json(orient='records')
            mydict['borehole'] = json.loads(myjson)

            ### manual dips
            df = pd.read_sql(sql=text(querystring_mandips),con=conn)
            df = df.loc[df['Location_Code'] == name]
            
            #Datetime processing
            df['Date_Time'] = pd.to_datetime(df['Date_Time'], yearfirst=True)
            df['Date_Time'] = df['Date_Time'].apply(lambda x: x.isoformat())

            df = df[['Date_Time', 'Water_Depth']]
            myjson = df.to_json(orient='records')
            mydict['depth_to_groundwater'] = json.loads(myjson)

        parsed = json.loads(json.dumps(mydict))
        return parsed

    except Exception as e:
        print('Error connecting to EsDAT database - you must be connected to the company network')
        print(e)
        return e


#Please note: error handling is not yet done on this app
@app.errorhandler(Exception)
def handle_exception(e):
    # pass through HTTP errors
    if isinstance(e, HTTPException):
        return e
    # now handling non-HTTP exceptions only
    if type(e) == sqlalchemy.exc.OperationalError:
        return 'SQL connection error'
    
    return 'error'


if __name__ == "__main__":
    app.run()
