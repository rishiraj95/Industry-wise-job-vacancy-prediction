import os
import psycopg2 as pg

filename='SalesJan2009.csv'
conn_cred="host=54.87.35.15 dbname=qcew user=postgres password=happy@1234"

conn=pg.connect(conn_cred)
cur=conn.cursor()

cur.execute(""" CREATE TABLE IF NOT EXISTS Sales (Transaction_date text, Product text, Price text, Payment_type text, Name text, City text, State text, Country text, Account_Created text, Last_login text, Latitude text, Longitude text);""")


file=open('./SalesJan2009.csv','r')
sql="copy sales from STDIN with csv header delimiter as ',';"
cur.execute('truncate sales;')
cur.copy_expert(sql=sql, file=file)
cur.close()
conn.commit()
conn.close()




"""
with open(filename) as f:
    next(f)
    cur.copy_from(f, 'Sales', sep=',')
    conn.commit()
"""

