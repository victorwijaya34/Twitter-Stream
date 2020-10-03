import pandas as pd
from pandas import DataFrame
import json

#change json to dataframe
df = pd.read_json (r'/home/pi/Kafka-Test/Data/part-00000-1dc86f67-f537-4197-8b32-ffe50708e888-c000.json', lines=True)
#bn = pd.DataFrame(df.value.values.tolist())
#pd.DataFrame.from_records(bn).head()


#lvl1 = df["value"]
#lvl2 = lvl1
newString = df.replace("\\","")
#wat = json_normalize(df)
newString.to_json(r'/home/pi/Kafka-Test/Dataku/cobajson.json')
#print(df["value"])
#print(newSr)


