# -*- coding: utf-8 -*-
"""
Created on Sun Nov  2 13:24:26 2014

@author: kobrien
"""
"""
DNSC6211 Group Project

This program loads various data sources into a database, creates a linked to table for the
datasources, and creates views to be used by the R Shiny User Interface program

The various datasources are:
    Crime Statistics for Department of Justice
    Places, latitude, longitude from Census
    Weather - average temperatures
    Per Diem Rates from gsa.gov
    Public Transportation 
    Airport Codes
    
    
"""
import urlparse
import urllib2
from bs4 import BeautifulSoup
import pandas as pd
import dbFunctions as db # i had db functions in a separate file, but moved to include as one program
import string
import matplotlib.pyplot as plt
import numpy as np
import MySQLdb
#import majorCities as mc


# database configuration parameters
config = {
  'user': 'root',
  'password': 'root',
  'host': '192.168.56.101',
  'raise_on_warnings': True,
}
DB_NAME = "groupV1"
dbconnectionstring = str("mysql+mysqldb://" + config['user'] + ":" + \
    config['password'] + "@" + config['host'] + "/" + DB_NAME)

baseDataFileDir = "/home/kobrien/googledrive/dnsc6211-programming/group project/data/"

#########################################################################
# main program
########################################################################  

# connect and setup the database
#connect to mysql instance
cnx = db.open_database(config)


#create a database
db.create_database(cnx,DB_NAME)
cnx.database = DB_NAME

from sqlalchemy import create_engine
engine = create_engine(dbconnectionstring)





#majorCitiesDF = mc.create_major_city_df()
#majorCitiesDF.to_sql("majorCities", engine, if_exists = 'replace')
masterTransport = pd.read_excel(baseDataFileDir + "March 2014 Raw Database.xls", 'MASTER')
masterTransport.to_sql("stage_transport", engine,if_exists = 'replace')



placesDF = pd.read_csv(baseDataFileDir + "2014_Gaz_place_national.txt", sep='\t')

# strip trailing whitespace and rename the column to prevent database error
longitude = placesDF.columns[11].rstrip()
placesDF.rename(columns={ placesDF.columns[11]:longitude }, inplace=True)
for i in placesDF.index:
    placesDF['city'][i] = (placesDF['NAME'][i].rsplit(" ",1))[0]
placesDF.to_sql("places", engine, if_exists = 'replace')
###### process the per diem file ########
# per diem downloaded at: https://inventory.data.gov/dataset/ad729937-b245-4eec-a88d-b72eb36d8106/resource/996f733b-7f9c-4011-a1a0-9768f67c1623/download/perdiemreimbursementrates.csv

# skip the first row as it is the data set title
#perDiemDF = pd.read_excel(baseDataFileDir + "FY2015_PerDiemRatesMasterFile.xlsx", header=1, skiprows=['0'])
perDiemDF = pd.read_csv(baseDataFileDir + "perdiemreimbursementrates.csv")
perDiemDF.to_sql("stage_perDiem", engine,if_exists = 'replace')



perDiemDF = perDiemDF.query('FiscalYear == 2015')
perDiemDF = perDiemDF[['DestinationID','City', 'FiscalYear', 'Oct', 'Nov', \
'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', \
'Meals', 'State']]
perDiemDF.drop_duplicates(['DestinationID','City', 'FiscalYear', 'Oct', 'Nov', \
'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', \
'Meals', 'State'], inplace=True)



citylist = list()
for i in perDiemDF.index:
    city = perDiemDF.get_value(i,'City')
    city = str.split(str(city),"/",1)[0].rstrip()
    # correct city name for Washington DC
    if (city == 'District of Columbia'):
        city = 'Washington'
    citylist.append(city)
    
perDiemDF['city']= citylist

perDiemDF['lat'] = 0.0
perDiemDF['long'] = 0.0

perDiemDF = perDiemDF[['DestinationID','city', 'State','FiscalYear', 'Oct', 'Nov', \
'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', \
'Meals', 'lat', 'long']]



#perDiemDF.rename(columns= {perDiemDF.columns[1]: 'City'}, inplace4=True)
#perDiemDF.rename( columns = { perDiemDF.columns[6]: 'LodgeRate', perDiemDF.columns[7]: 'MealRate' }, inplace=True)
perDiemDF.to_sql("perDiem", engine, if_exists = 'replace')


####### process the FBI crime stats file  ################################
crimeDF = pd.read_excel(baseDataFileDir + "table-6.xls", header=3, skipsrows =2)
crimeDF.to_sql("stage_crime", engine, if_exists = 'replace')
#crimeDF['rownum'] = list(range(len(crimeDF.index)))
crimeDF = crimeDF.drop(crimeDF.tail(6).index)
crimeDF.rename(columns={ 'Violent\ncrime': 'ViolentCrime', 'Property\ncrime': 'PropertyCrime' }, inplace=True)
crimeDF = crimeDF[['Metropolitan Statistical Area', 'Counties/principal cities','Population','ViolentCrime', 'PropertyCrime']] 

# Copy values and drop extra lines so that each MSA only has one row with needed crime stats
droplist = list()  # list of rows that will be deleted
for i in crimeDF.index:
    
    # if the first time MSA is filled in save it to copy to other rows
    if (pd.notnull(crimeDF['Metropolitan Statistical Area'][i])):
        currentMSA = crimeDF.get_value(i,'Metropolitan Statistical Area')
        
        msaPopulation = crimeDF.get_value(i, 'Population')
       # print "i=", i, "MSA=", currentMSA
    else:
        # set the empty MSA value to the current MSA
        crimeDF['Metropolitan Statistical Area'][i] = currentMSA
        crimeDF['Population'][i] = msaPopulation

    # if not the row with the summary stats then add to the list ot drop
    if (crimeDF['Counties/principal cities'][i] != 'Rate per 100,000 inhabitants'):
      #  print "Dropping: " , crimeDF['Counties/principal cities'][i], "index= ", crimeDF.index[i]
        droplist.append(i)
        
    #print "Droplist= " , droplist
    
# drop the rows that are not needed
crimeDF = crimeDF.drop(droplist)

# create a city and state columns from the MSA name
droplist = list()
cityList = list()
stateList = list()
for i in crimeDF.index:
    
    tempMSA= crimeDF.get_value(i,'Metropolitan Statistical Area')
    #    tempMSA = str.rstrip(str(tempMSA), "M.S.A")
    #    tempList = str.rsplit(tempMSA, ",",1)
    tempList = str.split(str(tempMSA), ",",1)

    # detect M.D. areas as we are only interested in MSAs
    if ( 'M.D.' in tempList[1]):
        droplist.append(i)
        
    state = (tempList[1].split("-",1))[0].strip() #remove whitespace
    state = (state.split())[0].strip()    
    city = (tempList[0].split("-",1))[0].strip()
    #print "tempMSA= ", tempMSA, "city= ", city, "state= ", state    
    cityList.append(city)
    stateList.append(state)
    
    
    
crimeDF['city'] = cityList
crimeDF['state'] = stateList
crimeDF['lat'] = 0.0  # placeholder columns for latitude
crimeDF['long'] = 0.0 # placeholder column for longitude

# drop the M.D. areas
crimeDF = crimeDF.drop(droplist)

crimeDF = crimeDF[['Metropolitan Statistical Area','Population','ViolentCrime', 'PropertyCrime', 'city', 'state', 'lat', 'long']] 


crimeDF.to_sql("crime", engine, if_exists = 'replace')


    
###### process the airport codes ###############################
airportsDF = pd.read_csv(baseDataFileDir + "airports.csv")
airportsDF.to_sql("stage_airports", engine,if_exists = 'replace')

airportsDF = airportsDF.query('iso_country == "US"  ')
airportsDF = airportsDF.query('type in ("large_airport", "medium_airport", "small_airport")')
airportsDF = airportsDF.query('scheduled_service == "yes"  ')
airportsDF = airportsDF[['iata_code', 'name', 'municipality', 'iso_region', 'type']]

stateList = list()

for i in airportsDF.index:
    state = airportsDF.get_value(i,'iso_region').split("-",1)[1]
    stateList.append(state)
   
        
airportsDF['state'] = stateList


# save results to database
airportsDF.to_sql("airports", engine, if_exists = 'replace')

 

# make adjustments for airports that have multiple international airports
cursor = cnx.cursor()
ddl = "delete from airports where iata_code = 'BFI'" # correct Seattle
cursor.execute(ddl)
ddl = "delete from airports where iata_code = 'HOU'" # correct Houston
cursor.execute(ddl)
ddl = "delete from airports where iata_code = 'MDW'"  # correct Chicago
cursor.execute(ddl)
ddl = "delete from airports where iata_code = 'LGA'"  # correct New York
cursor.execute(ddl)
ddl = "delete from airports where iata_code = 'SFB'" # correct Orlando
cursor.execute(ddl)
ddl = "delete from airports where iata_code = 'DAL'" # correct Dallas
cursor.execute(ddl)
ddl = "delete from airports where iata_code = 'DCA'" # correct Washington
cursor.execute(ddl)
ddl = "update airports set municipality= 'Dallas' where iata_code = 'DFW'"
cursor.execute(ddl)
ddl = "update airports set iata_code = 'WAS' where iata_code = 'IAD'"
cursor.execute(ddl)
cursor.execute('commit')  

cursor.close()
############################ end of airports ##############################


######  load weather data  ################

stationdata=pd.read_csv(baseDataFileDir + "mly-tavg-normal-final.csv")
stationdata.to_sql('avgtempdata', engine, if_exists='replace')
               
####### end of weather data ################

######  load weather data  ################

transportdata=pd.read_excel(baseDataFileDir + "Transportation.xlsx")
transportdata.to_sql('transportdata', engine, if_exists='replace')
               
####### end of weather data ################

### create the ref city table
ddl = " as (  \
select p.geoid 'placesGeoId', c.index 'crimeTableIndex', c.city, c.state, p.INTPTLAT 'lat', p.INTPTLONG 'long' \
from places p, crime c \
where p.city = c.city and p.usps = c.state \
)"
db.create_table(cnx, "ref_city", ddl)

### lat/long back into crime table
ddl = " update crime, ref_city  \
set crime.lat = ref_city.lat   , crime.long = ref_city.long \
where crime.index = ref_city.crimeTableIndex "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')


# add perDiem key into ref_city
ddl = " alter table ref_city ADD perDiemIndex BIGINT "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')

# update lat/longs for perDiem
ddl = " update perDiem , ref_city  \
set perDiem.lat = ref_city.lat   , perDiem.long = ref_city.long, ref_city.perDiemIndex = perDiem.index \
where perDiem.city = ref_city.city and perDiem.state = ref_city.state "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')

#######    update ref_city with airport Index
# add airport key into ref_city
ddl = " alter table ref_city ADD airportIndex BIGINT "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')
## populate the airport key into ref_city
ddl = " update  airports, ref_city  \
set  ref_city.airportIndex = airports.index \
where airports.municipality = ref_city.city and airports.state = ref_city.state "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')

# add weather key into ref_city
ddl = " alter table ref_city ADD weatherIndex BIGINT "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')
# populate the weather key into ref_city
ddl = " update  avgtempdata, ref_city  \
set  ref_city.weatherIndex = avgtempdata.index \
where avgtempdata.City = ref_city.city and avgtempdata.State = ref_city.state "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')

# add transportation key into ref_city
ddl = " alter table ref_city ADD transportationIndex BIGINT "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')
# populate the weather key into ref_city
ddl = " update  transportdata, ref_city  \
set  ref_city.transportationIndex = transportdata.index \
where transportdata.HQCity = ref_city.city and transportdata.HQState = ref_city.state "
cursor = cnx.cursor()
cursor.execute(ddl)
cursor.execute('commit')


#ddl = " CREATE VIEW v_cities AS \
#  SELECT r.city, r.state, r.lat, r.long, cr.Population, cr.ViolentCrime, cr.PropertyCrime, pd.Meals, pd.Jan 'Lodging' \
#  from ref_city r, crime cr, perDiem pd  \
#  where r.crimeTableIndex = cr.index and r.perDiemIndex = pd.index  "
  
ddl = "  CREATE VIEW v_cities AS \
  SELECT r.city, r.state, r.lat, r.long, a.iata_code 'airportCode', cr.Population, cr.ViolentCrime, cr.PropertyCrime, pd.Meals, pd.Jan 'Lodging', w.Jan 'Temperature', t.PassengerMilesFY 'PassengerMiles' \
  from ref_city as r \
  left join crime as cr on r.crimeTableIndex = cr.index \
  left join perDiem as pd  on r.perDiemIndex = pd.index \
  left join airports as a on r.airportIndex = a.index  \
  left join avgtempdata as w on r.weatherIndex = w.index  \
  left join transportdata as t on r.transportationIndex = t.index"
cursor.execute(ddl)
cursor.execute('commit')
  
  
#ddl = " CREATE VIEW v_perDiem AS \
#  SELECT pd.* \
#  from ref_city r, perDiem pd \
#  where r.perDiemIndex is not null and r.perDiemIndex = pd.index "

ddl = " CREATE VIEW v_perDiem AS \
  SELECT pd.* \
  from ref_city as r \
  left join perDiem as pd \
  on r.perDiemIndex = pd.index "
cursor.execute(ddl)
cursor.execute('commit') 

ddl = " CREATE VIEW v_temperature AS \
  SELECT w.* \
  from ref_city as r \
  left join avgtempdata as w \
  on r.weatherIndex = w.index "
cursor.execute(ddl)
cursor.execute('commit') 


