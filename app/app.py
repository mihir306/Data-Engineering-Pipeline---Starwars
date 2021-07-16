# importing libraries
import json
import requests
import pandas as pd
import re
import sqlalchemy
from faker import Faker
import numpy as np
import random
import string
import psycopg2



# defined required constant values/link
GetURLStar='https://swapi.dev/api/starships/'
GetURLfilms="https://swapi.dev/api/films/"
respStarshipList = []
respStarshipAndFilmRelationList = []
respFilmsList = []
filmUrlRegex ="https:\/\/swapi.dev\/api\/films\/(\d+)"


# creating starwars_dw schema in dw database
postgresConnection = psycopg2.connect(database="dw", user="starship", password="password321", host="db", port="5432")
print("Database opened successfully")
cur = postgresConnection.cursor()
cur.execute('''create schema IF NOT EXISTS starwars_dw''')
postgresConnection.commit()

# creating sales_db schema in sales_db database
postgresConnection = psycopg2.connect(database="salesdb", user="starship", password="password321", host="db", port="5432")
print("Database opened successfully")
cur = postgresConnection.cursor()
cur.execute('''create schema IF NOT EXISTS sales_db''')
postgresConnection.commit()


# This function is to GET requests calls and return received Response.
def callAPI(link):
    #print(link)
    response = requests.get(link)
    return response

def buildStarShipFilmRelationList(idx,filmList):
    for filmUrl in filmList:
        filmId = re.search(filmUrlRegex, filmUrl)
        if filmId:
            ssDict = {"starshipid":idx, "f_id":filmId.group(1)}
            respStarshipAndFilmRelationList.append(ssDict)
            getFilmsList(filmId.group(1),filmUrl)
        else:
            print("No Film-Id Found !! ")

            
# This function is to process Film API response and add details to films response list
def getFilmsList(f_id,filmUrl):
            film_Response= callAPI(filmUrl)
            # don't process further if API response is not success
            if(film_Response.status_code != 200):
                print(film_Response.status_code)
                pass
            filmJsonResponse = json.loads(film_Response.text)
            filmJsonResponse['f_id']= f_id
            respFilmsList.append(filmJsonResponse)
            
# Program Execution : START here
starshipIds = range(2, 16)
for i in starshipIds:
    # invokde GET api to fetch Startship details
    apiResponse = callAPI(GetURLStar + str(i) + "/")
    # don't process further if API response is not success
    if(apiResponse.status_code != 200):
        print(apiResponse.status_code)
        continue
    jsonResponse = json.loads(apiResponse.text)
    jsonResponse['starshipid']= i
    #print(jsonResponse)
    buildStarShipFilmRelationList(i,jsonResponse.get('films'))
    jsonResponse.pop('films',None)
    respStarshipList.append(jsonResponse)
    
# Creating StartShip DataFrame
starShipDF=pd.DataFrame(respStarshipList)
starShipDF=starShipDF[['starshipid','name', 'model','manufacturer', 'crew','passengers','starship_class']]
print(starShipDF)

# Creating StarShip-Film DataFrame
starshipAndFilmRelDF=pd.DataFrame(respStarshipAndFilmRelationList)
print(starshipAndFilmRelDF)

# Creating Film DataFrame
filmsDF=pd.DataFrame(respFilmsList)
filmsDF=filmsDF[['f_id','title','release_date']].drop_duplicates(['f_id','title','release_date'])
print(filmsDF)


# loading tables dw
engine = sqlalchemy.create_engine("postgresql://starship:password321@db/dw")
con = engine.connect()
print(engine.table_names())

table_name1 ='STARSHIP_FILM_RELATION_T'
starshipAndFilmRelDF.to_sql(table_name1, con ,schema='starwars_dw',if_exists='replace')

table_name3 ='FILMS_DETAILS_T'
filmsDF.to_sql(table_name3, con ,schema='starwars_dw',if_exists='replace')


# Salesdb  - loading fake data in sales_db
result=[]
fake = Faker()
nrow = 10
salseDBDF = pd.DataFrame()

salseDBDF['email'] = [fake.email()
              for _ in range(nrow)]

data = np.random.randint(5,30,size=20)
salseDBDF['quantity'] = pd.DataFrame(data, columns=['quantity'])

data = np.random.randint(1,3,size=20)
salseDBDF['price'] = pd.DataFrame(data, columns=['quantity'])

salseDBDF['sales_rep'] = [fake.email()
              for _ in range(nrow)]

for i in range(10):
    result_str = ''.join(random.sample(string.ascii_lowercase, 4))
    result .append(result_str)

salseDBDF['promo_code'] = result

salseDBDF[['poster_content','starshipid']]= starShipDF[['name','starshipid']].sample(n=9)

salseDBDF


engine = sqlalchemy.create_engine("postgresql://starship:password321@db/salesdb")
con = engine.connect()
table_name1 ='STARSHIP_POSTER_SALES_T'
salseDBDF.to_sql(table_name1, con ,schema='sales_db',if_exists='replace')


result = pd.merge(starShipDF, salseDBDF[['quantity','price','promo_code','starshipid','poster_content']],how='inner' ,left_on=['starshipid','name'], right_on=['starshipid','poster_content'], )
del result["poster_content"]
result

engine = sqlalchemy.create_engine("postgresql://starship:password321@db/dw")
con = engine.connect()
table_name = 'STARSHIP_DETAILS_T'
result.to_sql(table_name, con ,schema='starwars_dw',if_exists='replace')


# Summarizing the data :
con = psycopg2.connect(database="dw", user="starship", password="password321", host="db", port="5432")
print("Database opened successfully")
cur = con.cursor()
cur.execute('''with cte as (
select A.starshipid,string_agg(B.title ||' : '|| B.release_date,';')  as titleR from starwars_dw."STARSHIP_FILM_RELATION_T" A
join starwars_dw."FILMS_DETAILS_T" B
on A.f_id=B.f_id
group by A.starshipid)
select A.*,B.titleR from starwars_dw."STARSHIP_DETAILS_T" A
join cte B on A.starshipid=B.starshipid;''')
rows = cur.fetchall()
print(rows)
