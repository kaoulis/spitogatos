# Context:
# SpaN provides an online portal for real estate services.
# Main functionality: Filtered Search
## Imports and Settings
import sqlite3
import pandas as pd
import numpy as np
import json
import os
import ast
from pathlib import Path

pd.set_option('display.max_rows', 2000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 300)

## Path Configuration
basedir = os.path.dirname(os.path.abspath(__file__))
geo_path = os.path.join(basedir, 'data/geography.sqlite')
searches_path = os.path.join(basedir, 'data/spitogatos_data_engineer_assignment_raw_data')

rootdir = Path(searches_path)
file_list = [f for f in rootdir.glob('**/*') if f.is_file()]

## Alt Option1: Very very slow implementation... But skips json parsing errors
# with open(file_list[0], 'r') as fl:
#     lines = fl.readlines()
#     for l in lines:
#         search = ast.literal_eval(l)
#         df = df.append(search, ignore_index=True)
#     df = df.append(search, ignore_index=True)

## Alt Option2: For previous pandas versions that did not support json lines
# jsonfile1 = [json.loads(line.replace("'", '"').replace("False", "false").replace("True", "true")) for line in open(test_path, 'r')]

## Load and merge Searches data in Pandas dataframe
searches_df = None
for f in file_list:
    with open(f, 'r') as fl:
        json_file = fl.read().replace("'", '"').replace("False", "false").replace("True", "true")
        curr_df = pd.read_json(json_file, lines=True)
        searches_df = pd.concat([searches_df, curr_df], ignore_index=True)

##
filters = ['date', 'areaIDs', 'category', 'listingType', 'livingAreaLow', 'livingAreaHigh', 'priceLow', 'priceHigh',
           'newDevelopment', 'garage', 'storage', 'balcony', 'secureDoor', 'alarm', 'fireplace', 'elevator', 'garden',
           'roomsLow', 'roomsHigh', 'petsAllowed']

searches_df = searches_df[filters]

## Load Geo data in Pandas dataframe
con = sqlite3.connect(geo_path)
cur = con.cursor()

geo_df = pd.read_sql_query("SELECT * from geography", con)
geo_df.set_index('geographyId', inplace=True, drop=False, append=False)

con.close()

## Create countryId, regionId columns in geo_df (alternate to SQL recursive CTE)
countryIDs = geo_df[geo_df['country_flag'] == 1]['geographyId']
regionIDs = geo_df[geo_df['region_flag'] == 1]['geographyId']


def getCountryID(geo_id):
    if geo_id in countryIDs:
        return geo_id
    return getCountryID(geo_df['parentId'][geo_id])


def getRegionID(geo_id):
    if geo_id in regionIDs:
        return geo_id
    if geo_id in countryIDs:
        return 0
    return getRegionID(geo_df['parentId'][geo_id])


geo_df['countryId'] = geo_df.geographyId.map(getCountryID)
geo_df['regionId'] = geo_df.geographyId.map(getRegionID)

## Merging and finalising the geo_df
country_df = geo_df[geo_df['country_flag'] == 1][['geographyId', 'name']]
region_df = geo_df[geo_df['region_flag'] == 1][['geographyId', 'name']]
# country_df.iloc[1]['name']

geo_df = pd.merge(geo_df, country_df, how="left", left_on="countryId", right_index=True)
geo_df = pd.merge(geo_df, region_df, how="left", left_on="regionId", right_index=True)

geo_df.drop(['geographyId_y', 'geographyId'], axis=1, inplace=True)
geo_df = geo_df.rename(columns={'geographyId_x': 'geographyId', 'name_x': 'geographyName',
                                'name_y': 'countryName', 'name': 'regionName'})

## Enriching the searches_df with geo data
# We explode by areaIDs because a property search can be performed in multiple areas at once.
test_df = searches_df.explode('areaIDs')

# Join geo data
test_df = pd.merge(test_df, geo_df, how="left", left_on="areaIDs", right_index=True)
test_df.drop(['geographyId', 'level', 'parentId', 'region_flag', 'country_flag', 'countryId', 'regionId'],
             axis=1, inplace=True)

# Dropping searches with null countryName because analysis of the data is going to be performed on a per country basis.
test_df = test_df[test_df['countryName'].notna()]

test_df.insert(loc=0, column='search_id', value=test_df.index)

##
res = test_df.groupby(['date', 'search_id', 'countryName', 'regionName', 'category', 'listingType', 'livingAreaLow',
                       'livingAreaHigh', 'priceLow', 'priceHigh', 'newDevelopment', 'garage', 'storage', 'balcony',
                       'secureDoor', 'alarm', 'fireplace', 'elevator', 'garden',
                       'roomsLow', 'roomsHigh', 'petsAllowed'], as_index=False, dropna=False)[
    ['areaIDs', 'geographyName']].aggregate(lambda x: list(x))
