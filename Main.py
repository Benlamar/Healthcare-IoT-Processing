from RawDataModel import RawDataModel
from datetime import datetime

table_name = "raw_data_table"
primary_key = "BSM_G101"

# rawdata = RawDataModel(table_name,primary_key,"1646648995566")
# rawdata.getTable()



# sortkey = datetime.fromisoformat("2022-03-07 17:03:04.050661")
# timestamp = int(round(sortkey.timestamp()*1000))
# print(timestamp)


sortkey = datetime.fromisoformat("2022-03-07 17:03:04.050661")
minute = sortkey.strftime("%M")
print(int(minute))