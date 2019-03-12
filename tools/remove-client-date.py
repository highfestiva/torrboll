import re,sqlite3

clear_date = re.compile(r'(\(\d+/\d+/\d+\)|\d+/\d+/\d+)')

db = sqlite3.connect('backup-log.db')
bad_clients = db.execute("SELECT CLIENT FROM BACKUP_LOG WHERE CLIENT LIKE '%/%'").fetchall()
cursor = db.cursor()
print(bad_clients)
for bad_client in bad_clients:
    bad_client = bad_client[0]
    client = clear_date.sub('', bad_client).strip()
    cursor.execute("UPDATE BACKUP_LOG SET CLIENT='%s' where CLIENT='%s';" % (client, bad_client))

cursor.execute('COMMIT')
