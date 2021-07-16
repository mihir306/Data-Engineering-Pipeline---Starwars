#genration of salesDB with automatic genration of all the columns except film list and ss_id
import pandas as pd
from faker import Faker
import numpy as np
import random
import string

#import psycopg2 as pg
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 
import sqlalchemy


engine = sqlalchemy.create_engine("postgresql://postgres:NEWUSER123456#@localhost/dw")
con = engine.connect()
print(engine.table_names())
#handel if the data alreay exists.
table_name = 'STARSHIP_DETAILS_T'
starShipDF.to_sql(table_name, con ,schema='starwars_dw',if_exists='replace')
le_name1 ='STARSHIP_FILM_RELATION_T'
starshipAndFilmRelDF.to_sql(table_name1, con ,schema='starwars_dw',if_exists='replace')
table_name3 ='FILMS_DETAILS_T'
filmsDF.to_sql(table_name3, con ,schema='starwars_dw',if_exists='replace')



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
    # get random string of length 4 without repeating letters
    result_str = ''.join(random.sample(string.ascii_lowercase, 4))
    result .append(result_str)

salseDBDF['promo_code'] = result

salseDBDF[['poster_content','starshipid']]= starShipDF[['name','starshipid']].sample(n=9)

salseDBDF

engine = sqlalchemy.create_engine("postgresql://postgres:NEWUSER123456#@localhost/salesdb")
con = engine.connect()
table_name1 ='STARSHIP_POSTER_SALES_T'
salseDBDF.to_sql(table_name1, con ,schema='sales_db',if_exists='replace')

##creation of customer table merging two datasets:salesDB and above creatd starship DF
result = pd.merge(starShipDF, salseDBDF[['quantity','price','promo_code','starshipid','poster_content']],how='inner' ,left_on=['starshipid','name'], right_on=['starshipid','poster_content'], )
del result["poster_content"]
result

#creation of starship table
engine = sqlalchemy.create_engine("postgresql://postgres:NEWUSER123456#@localhost/dw")
con = engine.connect()
table_name = 'STARSHIP_DETAILS_T'
result.to_sql(table_name, con ,schema='starwars_dw',if_exists='replace')
