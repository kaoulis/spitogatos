# Context:
# SpaN provides an online portal for real estate services.
# Main functionality: Filtered Search

import sqlite3
import pandas as pd
import json

pd.set_option('display.max_rows', 2000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 300)

##
con = sqlite3.connect('C:/Users/k_chi/PycharmProjects/spitogatos/data/geography.sqlite')
cur = con.cursor()

df = pd.read_sql_query("SELECT * from geography", con)

con.close()

##
df.head()

##
file_path = 'C:/Users/k_chi/PycharmProjects/spitogatos/data/spitogatos_data_engineer_assignment_raw_data/2021-04-20-12/tsm_property_searches_stream_prod-8-2021-04-20-12-01-07-15883f39-3f29-49f3-9aba-f8408c64a20b'
f = open(file_path, "r")
test = f.read()
test = test.replace("\'", "\"")
##
result = [json.loads(jline) for jline in test.splitlines()]

##

jsonObj = pd.read_json(path_or_buf=file_path, lines=True, encoding="ISO-8859-1")

##
import jsonlines

with jsonlines.open(file_path) as f:

    for line in f.iter():
        line.replace("\'", "\"")
        print(line['doi']) # or whatever else you'd like to do

##

