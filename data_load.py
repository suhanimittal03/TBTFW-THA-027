import psycopg2 as pg2 

conn = pg2.connect(database="postgres", user='postgres', password='Suhani@03', host="localhost", port='5432')

cursor = conn.cursor()

# cursor.execute("select version()")
cursor.execute("select * from AAPL")

data = cursor.fetchone()
print(data)

conn.close()
