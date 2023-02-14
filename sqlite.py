import csv,collections
import pandas as pd
import sys
import sqlite3
import itertools
from operator import itemgetter

df =  pd.read_csv(r'file.csv')
conn = sqlite3.connect('exports_dats.sqlite')

cur = conn.cursor()

cur.execute('CREATE TABLE IF NOT EXISTS EXPORTS ({})' .format(' ,'.join(df.columns)))


for rows in df.iterrows():
    sql = ('INSERT INTO EXPORTS ({}) VALUES ({})'.format(' ,'.join(df.columns), ','.join(['?']*len(df.columns))))
    cur.execute(sql, tuple(rows[1]))
     
conn.commit()

df1 = pd.read_sql('Select * from EXPORTS',conn)
print(df1)
df1['month'] = pd.to_datetime(df1['month'].astype('datetime64[ns]'))
df2 = df.sort_values(by='month')
print(df2)

#Number of transactions done in a month
Monthly_Transactions = df1.groupby(['month'],sort=True)['resale_price'].count()
print(Monthly_Transactions)

#Part three of Question 1
df1['town'] = pd.DataFrame(df['town'])
df2 = df1.groupby(['town'])['transaction_id'].count()
df2 = df2.sort_values(ascending=False)
print(df2.head(3))
