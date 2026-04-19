import sqlite3
import csv

conn = sqlite3.connect('islandlink_analysis.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]

for t in tables:
    if t == 'sqlite_sequence': continue
    cursor.execute(f'SELECT * FROM [{t}]')
    cols = [d[0] for d in cursor.description]
    with open(f'{t}.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(cursor.fetchall())
conn.close()
