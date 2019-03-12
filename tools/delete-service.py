import sqlite3

db = sqlite3.connect('backup-log.db')
cursor = db.cursor()
cursor.execute("DELETE FROM BACKUP_LOG WHERE SERVICE='Storage Craft'")
cursor.execute('COMMIT')
