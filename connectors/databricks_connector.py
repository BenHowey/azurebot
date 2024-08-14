
from databricks import sql
import os

HEADERS = {
    "IncidentID": {"dtype": "INT", "description": "Unique ID of the incident (int)"},
    "ROSNumber": {"dtype": "STRING", "description": "The coast guard Incident number (str)"},
    "GIN_Number": {"dtype": "STRING", "description": "The RNLI incident number (str)"},
    "RecordStatus": {"dtype": "", "description": ""},
    "First_Information_Received": {"dtype": "STRING", "description": "The organisation that was first alerted of the incident (str)"},
    "Year": {"dtype": "INT", "description": "Year of the incident (int)"},
    "AssemblySignalDateTime": {"dtype": "STRING", "description": "Datetime that the signal was sent to rescue services (str)"},
    "Lifeboat_Station": {"dtype": "STRING", "description": "The name of the responding lifeboat station (str)"},
    "SAP_Station_ID": {"dtype": "INT", "description": "The internal SAP ID for the lifeboat station that responded (int)"},
    "Lifeboat_Type": {"dtype": "STRING", "description": "The lifeboat type that was used to perform the rescue. Either an all weather lifeboat(ALB) or inshore lifeboat (ILB) (str)"},
    "Lifeboat_Class": {"dtype": "STRING", "description": "The class or model name of lifeboat used in the incident. This is categorical data (str)"},
    "Lifeboat_Number": {"dtype": "STRING", "description": "The asset number of the boat used in the incident (str)"},
    "Lifeboat_Name": {"dtype": "STRING", "description": "The unique name of the boat used in the incident (str)"},
    "Incident_Latitude": {"dtype": "DOUBLE", "description": "Latitude of the incident (double)"},
    "Incident_Longitude": {"dtype": "DOUBLE", "description": "Longitude of the incident (double)"},
    "Launched": {"dtype": "INT", "description": "Boolean flag indicating if the lifeboat was launched (1) or stood down before launching (0) (int)"},
    "Time_To_Launch_minutes": {"dtype": "INT", "description": "The time for all crew to be on site and the lifeboat launched in minutes (int)"},
    "Was_the_Launch_delayed": {"dtype": "STRING", "description": "Description of anything that caused a delay in the launch of a lifeboat (str)"},
    "Arrived_On_Scene": {"dtype": "INT", "description": "Boolean flag to indicate if the lifeboat arrived at the scene of the incident (1) or was stood down before arriving (0) (int)"},
    "Time_To_Reach_Casualty_minutes": {"dtype": "INT", "description": "The time taken by the lifeboat between launching and arriving at the scene of the incident in minutes (int)"},
    "Launch_Outcome": {"dtype": "STRING", "description": "Description of the service provided by the lifeboat (str)"},
    "Lives_Saved": {"dtype": "INT", "description": "Number of lives saved by the lifeboat for the particular incident (int)"},
    "People_Aided": {"dtype": "INT", "description": "Number of people helped, either saving their life or to a lesser extent (int)"},
    "Lives_Lost": {"dtype": "INT", "description": "Number of people who lost their life in the incident (int)"},
    "Body_Recoveries": {"dtype": "INT", "description": "Number of bodies recovered by the lifeboat (int)"},
    "Fatalities": {"dtype": "INT", "description": "The number of fatalities in the incident with bodies not neccessarily recovered (int)"},
    "DRI": {"dtype": "DOUBLE", "description": "The calculated severity of the incident between 0 and 1, where 0 is the lowest severity and 1 is where there is direct threat to life or where lives were lost (double)"},
    "AIC_Group": {"dtype": "STRING", "description": "High level activity information of the people involved in the incident. (str)"},
    "AIC": {"dtype": "STRING", "description": "Categorical activity of persons involved in the incident, such as Paddleboard, Jet ski, Walker/runner (str)"},
    "AIC_Subtype": {"dtype": "STRING", "description": "Failure mechanism that caused the incident (str)"},
    "Wind_Force": {"dtype": "INT", "description": "Beufort scale wind force at the time of the incident (int)"},
    "SeaState": {"dtype": "STRING", "description": "Description of the sea state in meters in the form '{min} to {max}m {desc}', where the desc is a word descibing the seastate (str)"},
    "SeaStateNumeric": {"dtype": "INT", "description": "Numeric description of the sea state between 0 and 5, where 0 is calm and 5 is rough (int)"},
    "InDarkness": {"dtype": "STRING", "description": "Variable indicating if the lifeboat launch was in the dark or light (str)"},
    "Crew_Minutes": {"dtype": "INT", "description": "The number of minutes the crew were involved in the incident (int)"},
    "Vessel_Minutes": {"dtype": "INT", "description": "The number of minutes the lifeboat was involved in the incident (int)"},
    "Crew_on_service": {"dtype": "INT", "description": "The number of crew on board the lifeboat involved in the incident (int)"},
    "Shorecrew_on_service": {"dtype": "INT", "description": "The number of land based crew involved in the incident (int)"},
    "Max_Vessel_Length": {"dtype": "INT", "description": "Where there was a vessel involved in the incident (not the lifeboat), this is the approoximate length of the vessel requiring assistance from the lifebaot (int)"},
    "Tow": {"dtype": "BOOL", "description": "Boolean indicating if a vessel was towed by the lifeboat in the incident"},
    "Medevac": {"dtype": "BOOL", "description": "Boolean indicating if any casualties were airlifted away from the scene (int)"},
    "Craft_type_commercial_vessel": {"dtype": "INT", "description": "The number of commercial vessels involved in the incident (int)"},
    "Craft_type_fishing_vessel": {"dtype": "INT", "description": "The number of fishing vessels involved in the incident (int)"},
    "Craft_type_pleasure_craft": {"dtype": "INT", "description": "The number of pleasure craft involved in the incident (int)"},
    "Craft_type_people_other": {"dtype": "INT", "description": "The number of other vessels (ie. not fishing, commercial or pleasure craft) involved in the incident (int)"},
    "Craft_assisted": {"dtype": "INT", "description": "The total number of vessels assisted by the lifeboat for the incident (int)"},
}


class DatabricksConnector():
    def __init__(self):
        # generate the headers list
        headers_dtype_list = [f"{key} {value['dtype']}" for key, value in HEADERS.items()]
        self.headers_list = ", ".join(headers_dtype_list)
        # generate the headers description list
        headers_description_list = [f"{key} {value['description']}" for key, value in HEADERS.items()]
        self.headers_description_list = ", ".join(headers_description_list)
        self.table_name = 'dev_bronze.datahack.incident'
        self.database_type = 'databricks'
        self.additional_system_info = """
                Only use the following format for filtering strings: column_name ILIKE '%value%'.

                Any SQL commands that are related to dates or times, please convert date times to datetime objects using: 
                `TRY_CAST(TO_TIMESTAMP(<time_variable>, 'dd/MM/yyyy HH:mm') AS TIMESTAMP)`.

                Missing values are represented as 'NULL' in the data - please ignore these from any calculations and use TRY_CAST to ignore the values when casting to a different type. 

                """

    def query_source_data(self, query):
        with sql.connect(server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                         http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                         access_token=os.getenv("DATABRICKS_TOKEN")) as connection:

            with connection.cursor() as cursor:
                cursor.execute(query)
                # get the column headings
                column_names = [description[0] for description in cursor.description]
                # fetch all the results
                rows = cursor.fetchall()
                # zip the column names and the rows to create a dictionary
                result = [dict(zip(column_names, row)) for row in rows]

        cursor.close()
        return result
