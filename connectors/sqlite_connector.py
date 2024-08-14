import sqlite3 as sql
import sqlite3
import csv
import requests
import os


def create_db(path, headers_list, db_path):
    if not os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute(f'CREATE TABLE IF NOT EXISTS Incident ({headers_list})')

        with open(path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            for row in reader:
                placeholders = ",".join(["?"] * len(row))
                sql = f'INSERT INTO Incident VALUES ({placeholders})'
                c.execute(sql, row)

        conn.commit()
        conn.close()
    else:
        print('Database already exists')


HEADERS = {"OBJECTID": {"dtype": "PRIMARY KEY", "description": "Unique ID of the incident"},
         "ROSNumber": {"dtype": "TEXT", "description": "The coast guard Incident number"},
         "LifeboatStationNameProper": {"dtype": "TEXT", "description": "The name of the responding lifeboat station"},
         "AIC": {"dtype": "TEXT", "description": "Categorical activity of persons involved in the incident, such as Paddleboard, Jet ski, Walker/runner"},
         "YearofCall": {"dtype": "INT", "description": "Year of the incident"},
         "LifeboatClass": {"dtype": "TEXT", "description": "The class or model name of lifeboat used in the incident. This is categorical data"},
         "LifeboatNumber": {"dtype": "TEXT", "description": "The asset number of the boat used in the incident"},
         "RoSType": {"dtype": "TEXT", "description": "This is the lifeboat type that was used to perform the rescue. Either an all weather lifeboat(ALB) or inshore lifeboat (ILB)"},
         "CasualtyCategory": {"dtype": "TEXT", "description": "A generic one word category incident type"},
         "CasualtyTypeFull": {"dtype": "TEXT", "description": "A full desciption of the AIC category"},
         "ReasonforLaunch": {"dtype": "TEXT", "description": "A description of what went wrong and lead to the lifeboat being launched"},
         "OutcomeOfService": {"dtype": "TEXT", "description": "The outcome of the inciident and the service provided by the lifeboat"},
         "Activity": {"dtype": "TEXT", "description": "Summary of the AIC column"},
         "VisibilityAtIncident": {"dtype": "TEXT", "description": "Categorical visability at the site of the incident."},
         "WeatherAtIncident": {"dtype": "TEXT", "description": "Categorical weather conditions at the site of the incident."},
         "SeaConditionsAtIncident": {"dtype": "TEXT", "description": "Categorical sea conditions at the site of the incident."},
         "WindDirectionAtIncident": {"dtype": "INT", "description": "Wind direction at the site of the incident."},
         "VisibilityAtLaunch": {"dtype": "TEXT", "description": "Categorical visability at the site of the lifeboat station."},
         "WeatherAtLaunch": {"dtype": "TEXT", "description": "Categorical weather conditions at the site of the lifeboat station."},
         "SeaConditionsAtLaunch": {"dtype": "TEXT", "description": "Categorical sea conditions at the site of the lifeboat station."},
         "WindDirectionAtLaunch": {"dtype": "FLOAT", "description": "Wind direction at the site of the lifeboat station."},
         "x": {"dtype": "FLOAT", "description": "Longitude of the incident"},
         "y": {"dtype": "FLOAT", "description": "Latitude of the incident"},
         "Date_of_Launch": {"dtype": "DATE", "description": "Date of the incident with format: 2022/01/19"},
         "Time_of_Launch": {"dtype": "TIME", "description": "Time of the launch of the lifeboat with format: 08:42:00"},
         "Date_Time_of_Launch": {"dtype": "DATETIME", "description": "Datetime of the incident with format: 2022/01/24 17:43:00+00"},
         }


# create the database here so when it gets imported it runs and creates one instance of the db
DB_PATH = os.path.join(os.path.dirname(__file__), 'RNLI_Return_of_Service.db')
req = requests.get('https://opendata.arcgis.com/api/v3/datasets/7fe0801fbf6147cb8053050316f15631_1/downloads/data?format=csv&spatialRefId=4326&where=1%3D1')
# Check if the request was successful
csv_path = os.path.join(os.path.dirname(__file__), 'RNLI_Return_of_Service.csv')
if req.status_code == 200:
    with open(csv_path, 'wb') as file:
        file.write(req.content)

headers_dtype_list = [f"{key} {value['dtype']}" for key, value in HEADERS.items()]
headers_list = ", ".join(headers_dtype_list)
headers_description_list = [f"{key} {value['description']}" for key, value in HEADERS.items()]
headers_description_list = ", ".join(headers_description_list)
create_db(csv_path, headers_list, DB_PATH)


class SQLliteConnector():
    def __init__(self):
        self.database_path = DB_PATH

        self.table_name = 'Incident'

        self.database_type = 'sqlite'

        self.additional_system_info = 'When filtering on station names or locations always use case insensitive fuzzy matching'

        self.headers_description_list = headers_description_list

    def query_source_data(self, query):
        conn = sql.connect(self.database_path)
        c = conn.cursor()
        c.execute(query)
        # Fetch the column names from the cursor's description attribute
        column_names = [description[0] for description in c.description]

        # Fetch all the results
        rows = c.fetchall()

        # Combine column names with rows to create a list of dictionaries
        result = [dict(zip(column_names, row)) for row in rows]

        # Close the connection
        conn.close()
        # result = c.fetchall()
        return result


if __name__ == '__main__':
    conn = SQLliteConnector()
    print(conn.query_source_data('SELECT * FROM Incident LIMIT 5'))
