import pandas as pd
import psycopg2
from psycopg2 import sql
import uuid

df = pd.read_csv('list.csv')

try:
    connection = psycopg2.connect(
        dbname="ex_db",
        user="userName",
        password="12345678",
        host="localhost",
        port="1994"
    )
    
    cursor = connection.cursor()
    
    for i in range (0,1313):
        random_uuid = str(uuid.uuid4())
        
        insert_query = '''
        INSERT INTO hospital (id,name, type, specialized_field, organization_belonging_to,province , city,ipd)
        VALUES (%s,%s, %s, %s,%s, %s, %s,%s);
        '''
        data = (
            random_uuid,
            df.at[i,'col1'],
            df.at[i,'col2'],
            df.at[i,'col3'],
            df.at[i,'col4'],
            df.at[i,'col5'],
            df.at[i,'col6'],
            df.at[i,'col7'],
            df.at[i,'col8'],
            )
        
        print(data[1])
        cursor.execute(insert_query, data)
    
    connection.commit()
    

except Exception as e:
    print("Error while connecting to PostgreSQL", e)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")