# egen

STEPS IN "runME.py"

1. Create a function that creates and returns a engine (we can use this SQLITE engine variable as global)

2. Create a table metadata (with county table schema) which will be created in-memory using the engine variable

3. Create a function that creates county table. Input args: county list, spark dataframe (which has the extracted data)

STEPS TAKEN INTO PARSING THE API DATA

4. read the data from API and create a new JSON file for each day (using: "readfromAPI.py")

5. use spark to read the JSON and create a spark DataFrame only with the columns that we want

6. Repartition the DataFrame with county's so when run on a cluster, its processed in parallel

7. create a list of distinct COUNTY's sourcing from the spark DataFrame (step 5.)

8. create county table using the meta data (from step 2.)

9. load the county table from the function (from step 3.) using the dataFrame (from step 5.) and county list (from step 7.)

UNIT TESTING

10. take any county as example from the list of county's and we can test like below

  a. select few rows just so we can compare it with the API data
  b. get the count of all records for one/two county's
  
==========E-X-E-C-U-T-I-O-N==========

========================================================================

SPARK SUBMIT COMMAND, RUN THIS ON A CLUSTER with either YARN/IBM SPECTURM CONDUCTOR as CLUSTER MANAGER 

(I used local as I'm working on a local PC)

C:\spark\spark-3.0.1-bin-hadoop2.7\bin\spark-submit --master local --num-executors 10 --py-files C:\Users\sruth\IdeaProjects\egen\runME.py C:\Users\sruth\IdeaProjects\egen\runME.py

========================================================================

create a ".sh" file like below (ex: scheduleme.sh)

#!/bin/bash

echo "Running for:" `date +"%m-%d-%Y"`

C:\spark\spark-3.0.1-bin-hadoop2.7\bin\spark-submit --master local --num-executors 10 --py-files C:\Users\sruth\Desktop\EGEN\testme.py C:\Users\sruth\Desktop\EGEN\testme.py

========================================================================

RUN THE ABOVE ".sh" USING "crontab -e"

0 9 * * * C:\Users\sruth\Desktop\EGEN\scheduleme.sh >> log.out

========================================================================

Assumptions: there is only one date/record in the API for each county/day, hence used the same date for both LOADDATE and TESTDATE

TABLE SCHEMA

=============

TEST_DATE: Test Date

COUNTY: COUNTY (this is not needed, but I still included)

NEWPOSITIVES: New Positives

CUMULATIVEPOSITIVES: Cumulative Number of Positives

TOTALNUMBER: Total Number of Tests Performed

CUMULATIVENUMBER: Cumulative Number of Tests Performed

LOAD_DATE: LOAD_DATE (same as Test Date)


