import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder. \
    config('spark.sql.debug.maxToStringFields', 2000). \
    appName("Process the data from API").getOrCreate()

from pyspark.sql.functions import *
from sqlalchemy import Table, Column, Integer, String, MetaData

import sqlalchemy as sqla
import pandas as pd

#############################################################################################################
# CREATE A SQLLITE ENGINE VARIABLE - TO BE USED AS GLOBAL
#############################################################################################################
engine = ''

#############################################################################################################
# FUNCTION TO CREATE CREATESQLITECONNECTION FUNCTION THAT CREATES A ENGINE AND RETURNS
#############################################################################################################
def createSqliteConnection():
    global engine
    engine = sqla.create_engine(f"sqlite://", echo=False)
    return engine
#############################################################################################################

#############################################################################################################
# CALL THE CREATESQLITECONNECTION FUNCTION THAT RETURNS THE ENGINE
#############################################################################################################
createSqliteConnection()

#############################################################################################################
# FUNCTION TO CREATE TABLE FOR EACH COUNTY
#############################################################################################################
def createCountyCovidTable(name):
    meta = MetaData()
    covidInfo = Table(
        f'{name}', meta,
        Column('TEST_DATE', String, ),
        Column('NEWPOSITIVES', Integer),
        Column('CUMULATIVEPOSITIVES', Integer),
        Column('TOTALNUMBER', Integer),
        Column('CUMULATIVENUMBER', String),)

    # CREATE COUNTY TABLE USING THE GLOBAL ENGINE VARIABLE
    meta.create_all(engine)
#############################################################################################################


#############################################################################################################
# CREATE A FUNCTION TO LOAD INTO EACH TABLE IN DATABASE
#############################################################################################################
def loadToTable(countyList, dataframe):
    for county in countyList:
        pandaFrame = dataframe.where(dataframe['COUNTY'] == f"{county}").toPandas()
        # test = pandaFrame[pandaFrame.TEST_DATE == "2020-03-05T00:00:00"]
        test = pandaFrame[pandaFrame.TOTALNUMBER >= 0]
        test.to_sql(county, engine, if_exists='replace')
#############################################################################################################


#############################################################################################################
# READ FROM API
#############################################################################################################
from readfromAPI import readAPI
filename = readAPI()
#############################################################################################################

#############################################################################################################
# CREATE A SPARK DATA FRAME FROM THE JSON, DROP THE META DATA AND TAKE ONLY THE COLUMNS THAT WE WANT
#############################################################################################################
df = spark.read.json(filename, multiLine='true')
df0 = df.drop("meta")

df1 = df0.withColumn("data_flat", explode(col("data"))).drop("data")
df2 = df1.withColumn("TEST_DATE", df1.data_flat[8].substr(0, 10)). \
    withColumn("COUNTY", df1.data_flat[9]). \
    withColumn("NEWPOSITIVES", df1.data_flat[10].cast('float')). \
    withColumn("CUMULATIVEPOSITIVES", df1.data_flat[11].cast('float')). \
    withColumn("TOTALNUMBER", df1.data_flat[12].cast('float')). \
    withColumn("CUMULATIVENUMBER", df1.data_flat[13]). \
    drop("data_flat")

#############################################################################################################
# DF2 HAS THE ACTUAL DATA WHICH IS SPLIT BASED ON THE COLUMNS THAT WE WANT
#############################################################################################################
# df2.show(truncate=False)
# df2.printSchema()

#############################################################################################################
# CREATE A COUNTY LIST TO ITERATE THROUGH COUNTIES IN NEW YURK
#############################################################################################################
countyList = sorted(list(df2.select('COUNTY').distinct().toPandas()['COUNTY']))
# print(countyList)

#############################################################################################################
# CREATE TABLE FOR EACH COUNTY BY CALLING THE FUNCTION CREATECOUNTYCOVIDTABLE
#############################################################################################################
for county in countyList:
    createCountyCovidTable(county)

#############################################################################################################
# LOAD EACH COUNTY TABLE WITH DATA USING THE FUNCTION LOADTOTABLE
#############################################################################################################
loadToTable(countyList, df2)

#############################################################################################################
# UNIT TESTING
#############################################################################################################
# query = "select COUNT(*) from 'New York'"
query = "select * from 'New York' limit 10"
pd.set_option("display.max_column", None)
result = pd.read_sql(query, engine)
print(result)
#############################################################################################################

