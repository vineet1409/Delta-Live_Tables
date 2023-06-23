import time
import json
from csv import reader
from azure.eventhub import EventHubProducerClient, EventData

connection_str = 'Endpoint=sb://streamingeventhub2.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=rzs1q+a2bgXfiCgxv7cpiCoUZv4dO2stncZOGOVHq7o='
eventhub_name = 'airlinehub'

class Airline:
    def __init__(self, MONTH,DAY_OF_MONTH,DAY_OF_WEEK,OP_UNIQUE_CARRIER,TAIL_NUM,OP_CARRIER_FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN,ORIGIN_CITY_NAME,DEST_AIRPORT_ID,DEST,DEST_CITY_NAME,
                	CRS_DEP_TIME,DEP_TIME,DEP_DELAY_NEW,DEP_DEL15,DEP_TIME_BLK,CRS_ARR_TIME,ARR_TIME,ARR_DELAY_NEW,ARR_TIME_BLK,CANCELLED,CANCELLATION_CODE,
                    CRS_ELAPSED_TIME,ACTUAL_ELAPSED_TIME,DISTANCE,DISTANCE_GROUP,CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY):
        self.MONTH = MONTH
        self.DAY_OF_MONTH = DAY_OF_MONTH
        self.DAY_OF_WEEK = DAY_OF_WEEK
        self.OP_UNIQUE_CARRIER = OP_UNIQUE_CARRIER
        self.TAIL_NUM = TAIL_NUM
        self.OP_CARRIER_FL_NUM = OP_CARRIER_FL_NUM
        self.ORIGIN_AIRPORT_ID = ORIGIN_AIRPORT_ID
        self.ORIGIN = ORIGIN

        self.ORIGIN_CITY_NAME = ORIGIN_CITY_NAME
        self.DEST_AIRPORT_ID = DEST_AIRPORT_ID
        self.DEST = DEST
        self.DEST_CITY_NAME = DEST_CITY_NAME
        self.CRS_DEP_TIME = CRS_DEP_TIME
        self.DEP_TIME = DEP_TIME
        self.DEP_DELAY_NEW = DEP_DELAY_NEW
        self.DEP_DEL15 = DEP_DEL15

        self.DEP_TIME_BLK = DEP_TIME_BLK
        self.CRS_ARR_TIME = CRS_ARR_TIME
        self.ARR_TIME = ARR_TIME
        self.ARR_DELAY_NEW = ARR_DELAY_NEW
        self.ARR_TIME_BLK = ARR_TIME_BLK
        self.CANCELLED = CANCELLED
        self.CANCELLATION_CODE = CANCELLATION_CODE
        self.CRS_ELAPSED_TIME = CRS_ELAPSED_TIME

        self.ACTUAL_ELAPSED_TIME = ACTUAL_ELAPSED_TIME
        self.DISTANCE = DISTANCE
        self.DISTANCE_GROUP = DISTANCE_GROUP
        self.CARRIER_DELAY = CARRIER_DELAY
        self.WEATHER_DELAY = WEATHER_DELAY
        self.NAS_DELAY = NAS_DELAY
        self.SECURITY_DELAY = SECURITY_DELAY
        self.LATE_AIRCRAFT_DELAY = LATE_AIRCRAFT_DELAY
    def __str__(self):
        return f"{self.MONTH}, {self.DAY_OF_MONTH}, {self.DAY_OF_WEEK}, {self.OP_UNIQUE_CARRIER}, {self.TAIL_NUM}, {self.OP_CARRIER_FL_NUM}, {self.ORIGIN_AIRPORT_ID}, {self.ORIGIN}, {self.ORIGIN_CITY_NAME}, {self.DEST_AIRPORT_ID}, {self.DEST}, {self.DEST_CITY_NAME}, {self.CRS_DEP_TIME}, {self.DEP_TIME}, {self.DEP_DELAY_NEW}, {self.DEP_DEL15}, {self.DEP_TIME_BLK}, {self.CRS_ARR_TIME}, {self.ARR_TIME}, {self.ARR_DELAY_NEW}, {self.ARR_TIME_BLK}, {self.CANCELLED}, {self.CANCELLATION_CODE}, {self.CRS_ELAPSED_TIME},  {self.ACTUAL_ELAPSED_TIME}, {self.DISTANCE}, {self.DISTANCE_GROUP}, {self.CARRIER_DELAY}, {self.WEATHER_DELAY}, {self.NAS_DELAY}, {self.SECURITY_DELAY}, {self.LATE_AIRCRAFT_DELAY}"

def send_to_eventhub(client, data):
    event_data_batch = client.create_batch()
    event_data_batch.add(EventData(data))
    client.send_batch(event_data_batch)

def main():
    with open(r'C:\Azure Trainings\Project_Pro_Material\Azure_Databricks_Delta_Live_Tables\Airline_data\ONTIME_REPORTING_022.csv', 'r') as airline_desc:
        csv_reader = reader(airline_desc)
        header = next(airline_desc)

        client = EventHubProducerClient.from_connection_string\
                 (connection_str, eventhub_name=eventhub_name)
        for row in csv_reader:
            MONTH1 = row[0]
            DAY_OF_MONTH1 = row[1]
            DAY_OF_WEEK1 = row[2]
            OP_UNIQUE_CARRIER1 = row[3]
            TAIL_NUM1 = row[4]
            OP_CARRIER_FL_NUM1 = row[5]
            ORIGIN_AIRPORT_ID1 = row[6]
            ORIGIN1 = row[7]

            ORIGIN_CITY_NAME1 = row[8]
            DEST_AIRPORT_ID1 = row[9]
            DEST1 = row[10]
            DEST_CITY_NAME1 = row[11]
            CRS_DEP_TIME1 = row[12]
            DEP_TIME1 = row[13]
            DEP_DELAY_NEW1 = row[14]
            DEP_DEL151 = row[15]

            DEP_TIME_BLK1 = row[16]
            CRS_ARR_TIME1 = row[17]
            ARR_TIME1 = row[18]
            ARR_DELAY_NEW1 = row[19]
            ARR_TIME_BLK1 = row[20]
            CANCELLED1 = row[21]
            CANCELLATION_CODE1 = row[22]
            CRS_ELAPSED_TIME1 = row[23]

            ACTUAL_ELAPSED_TIME1 = row[24]
            DISTANCE1 = row[25]
            DISTANCE_GROUP1 = row[26]
            CARRIER_DELAY1 = row[27]
            WEATHER_DELAY1 = row[28]
            NAS_DELAY1 = row[29]
            SECURITY_DELAY1 = row[30]
            LATE_AIRCRAFT_DELAY1 = row[31]

            airportdur = Airline(MONTH1, DAY_OF_MONTH1, DAY_OF_WEEK1, OP_UNIQUE_CARRIER1, TAIL_NUM1, OP_CARRIER_FL_NUM1, ORIGIN_AIRPORT_ID1, ORIGIN1, ORIGIN_CITY_NAME1, DEST_AIRPORT_ID1, DEST1, DEST_CITY_NAME1, CRS_DEP_TIME1, DEP_TIME1, DEP_DELAY_NEW1, DEP_DEL151, DEP_TIME_BLK1, CRS_ARR_TIME1, ARR_TIME1, ARR_DELAY_NEW1, ARR_TIME_BLK1, CANCELLED1, CANCELLATION_CODE1, CRS_ELAPSED_TIME1,  ACTUAL_ELAPSED_TIME1, DISTANCE1, DISTANCE_GROUP1, CARRIER_DELAY1, WEATHER_DELAY1, NAS_DELAY1, SECURITY_DELAY1, LATE_AIRCRAFT_DELAY1)
            send_to_eventhub(client, json.dumps(airportdur.__dict__))
            print(json.dumps(airportdur.__dict__))

main()
