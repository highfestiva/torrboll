import sqlite3

db = sqlite3.connect('backup-log.db')
cursor = db.cursor()
for v in cursor.execute("SELECT * FROM BACKUP_LOG WHERE SERVICE NOT IN ('Hyper-V', 'CrashPlan PRO', 'Ahsay') LIMIT 10").fetchall():
    print(v)

