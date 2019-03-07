#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from base64 import b64decode as base64decode
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import dateutil.parser
from email import policy
from email.parser import BytesParser
from flask import Flask, render_template
import imaplib
import pandas as pd
import quopri
import sqlite3
import threading
import time


app = Flask(__name__)


def create_db(fname):
    db = sqlite3.connect(fname)
    try:
        c = db.cursor()
        c.execute('SELECT COUNT(*) FROM BACKUP_LOG;')
        c.fetchall()
    except:
        db.execute('''CREATE TABLE BACKUP_LOG(
                        ID         INTEGER PRIMARY KEY,
                        TIMESTAMP  DATETIME            NOT NULL,
                        SERVICE    CHAR(20)            NOT NULL,
                        CLIENT     CHAR(40)            NOT NULL,
                        SYSTEM     CHAR(40)            NOT NULL,
                        JOB        CHAR(20)            NOT NULL,
                        PERC       INT                 NOT NULL);''')
        db.execute('''CREATE UNIQUE INDEX idx_jobs ON BACKUP_LOG (TIMESTAMP, SERVICE, CLIENT, SYSTEM, JOB);''')
    return db


def imap_connect():
    server = open('imap-server.cfg').read().strip()
    u_p,h_p = server.rsplit('@',1)
    host,port = h_p.rsplit(':',1)
    user,passwd = u_p.split(':',1)
    imap_conn = imaplib.IMAP4_SSL(host,int(port))
    imap_conn.login(user, passwd)
    # for e in imap_conn.list()[1]:
        # print(e.decode())
    imap_conn.select()
    return imap_conn


def imap_disconnect(imap_conn):
    imap_conn.close()
    imap_conn.logout()


def split_subject(subject):
    splitstr = '-' if subject.count('-')==2 else ' - '
    words = [e.strip() for e in subject.split(splitstr)]
    if len(words) == 3:
        job,client,our_company = words
    else:
        job,client = words
        our_company = 'Björk IT'
    job = job.split('SUCCESS')[-1].strip()
    if 'Bjork' in client or 'Björk' in client:
        client,our_company = our_company,client
    return job,client


def parse_crashplan(subject, html):
    phtml = BeautifulSoup(html, features='html.parser')
    for system_row in phtml.find_all('tr', class_='lastForComputer'):
        tds = system_row.find_all('td')
        system = tds[0].text.split('→')[0].strip()
        perc = int(float(tds[3].text.strip('%')))
        yield system, system, system, perc


def parse_storage_craft(subject, html):
    subject = subject.split('Online Image Report:')[-1].strip()
    subject = subject.rsplit(' ', 1)[0]
    job,client = split_subject(subject)
    phtml = BeautifulSoup(html, features='html.parser')
    for system_row in phtml.find_all('table', cellspacing='15'):
        system = '?'
        perc = 0
        for td in system_row.find_all('td'):
            for span in td.findChildren('span'):
                system = span.text.strip()
                break
            style = td['style']
            style = [e.strip().split(':') for e in style.split(';')]
            style = {k:v for k,v in style}
            if 'border' in style:
                if '#5DE01B' in style['border']:
                    perc = 100
            yield client, system, job, perc


def parse_ahsay(subject, html):
    subject = subject.split('Backup Summary:')[-1].strip()
    subject = subject.rsplit(' ', 1)[0]
    job,client = split_subject(subject)
    phtml = BeautifulSoup(html, features='html.parser')
    for system_row in phtml.find_all('table', width='100%'):
        system = '?'
        perc = 0
        for td in system_row.find_all('td'):
            system = td.text.strip()
            system = system.split('Backupset:')[-1].strip()
            style = td['style']
            style = [e.strip().split(':') for e in style.strip('; ').split(';')]
            style = {k:v for k,v in style}
            if 'background-color' in style and style['background-color'] == '#FF9933':
                perc = 100
            yield client, system, job, perc
            break


def parse_hyper_v(subject, html):
    phtml = BeautifulSoup(html, features='html.parser')
    client = phtml.find_all('h2')[0].text.split("'")[1]
    for system_row in phtml.find_all('table'):
        system = '?'
        perc = 0
        for tr in system_row.find_all('tr')[1:]:
            tds = tr.find_all('td')
            system = tds[0].text.strip()
            perc = 100 if tds[2].text=='Operating normally' else 0
            yield client, system, system, perc
        break


def catalogue_mail(cursor, subject, timestamp, html):
    result = []
    if 'Code42' in subject and 'Backup Report' in subject:
        service = 'CrashPlan PRO'
        result = parse_crashplan(subject, html)
    elif 'Online Image Report' in subject:
        service = 'Storage Craft'
        result = parse_storage_craft(subject, html)
    elif 'Backup Summary:' in subject:
        service = 'Ahsay'
        result = parse_ahsay(subject, html)
    elif 'Hyper-V Server Report' in subject:
        service = 'Hyper-V'
        result = parse_hyper_v(subject, html)
    else:
        print('Junk mail?', subject)
    timestamp = timestamp2utc(timestamp)
    for client,system,job,perc in result:
        s = "INSERT OR IGNORE INTO BACKUP_LOG (TIMESTAMP, SERVICE, CLIENT, SYSTEM, JOB, PERC) values (DATETIME('%s'), '%s', '%s', '%s', '%s', %i);" % (timestamp, service, client, system, job, perc)
        cursor.execute(s)


def parse_all(imap_conn):
    db = sqlite3.connect('backup-log.db')
    cursor = db.cursor()
    _,mail_results = imap_conn.search(None, 'ALL')
    msg_ids = mail_results[0].split()
    for i,msgid in enumerate(msg_ids):
        # try:
        if True:
            _,data = imap_conn.fetch(msgid, '(RFC822)')
            msg = BytesParser(policy=policy.default).parsebytes(data[0][1])
            subject = msg['Subject']
            timestamp = dateutil.parser.parse(msg['Date']).timestamp()
            payloads = msg.get_payload()
            html = None
            mime = ''
            if type(payloads) == str:
                mime = msg['Content-Transfer-Encoding']
                html = payloads
            else:
                while True:
                    for payload in payloads:
                        mime = ''
                        for k,v in payload.items():
                            if k == 'Content-Type' and ('text/html' in v or 'multipart/alternative' in v):
                                html = payload.get_payload()
                            elif k == 'Content-Transfer-Encoding':
                                mime = v
                        if html:
                            break
                    if type(html) == str:
                        break
                    payloads = html
                    html = None
            if not html:
                continue
            if 'base64' in mime:
                html = base64decode(html).decode()
            if 'quoted-printable' in mime:
                html = quopri.decodestring(html)
            catalogue_mail(cursor=cursor, subject=subject, timestamp=timestamp, html=html)
        # except:
            # pass
    cursor.execute('COMMIT')
    print('All e-mails catalogued.')


def timestamp2utc(t):
    return datetime.utcfromtimestamp(t).isoformat() + 'Z'


def handle_mails():
    while True:
        try:
            imap_conn = imap_connect()
            parse_all(imap_conn)
            imap_disconnect(imap_conn)
        except Exception as e:
            print('Uh-oh!', type(e), e)

        target = datetime.today() + timedelta(hours=11)
        if target < datetime.now():
            target += timedelta(hours=24)
        time.sleep((target - datetime.now()).total_seconds())


@app.route('/latest-status')
def latest_status():
    db = sqlite3.connect('backup-log.db')
    df = pd.read_sql_query('SELECT * FROM BACKUP_LOG ORDER BY TIMESTAMP', db)
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.date
    data = []
    cache = {}
    for service,dfs in df.groupby('SERVICE'):
        data += [[service, []]]
        for timestamp,dft in dfs.groupby('TIMESTAMP'):
            ts = str(timestamp).split(' ')[0]
            if not ts.endswith('01'):
                ts = ts.split('-')[-1]
            dft = dft.sort_values(by=['CLIENT','SYSTEM'])
            for (client,system,job),dfg in dft.groupby(['CLIENT','SYSTEM','JOB']):
                if (service,client,system,job) not in cache:
                    l = cache[(service,client,system,job)] = []
                    data[-1][-1].append((client,system,job, l))
                else:
                    l = cache[(service,client,system,job)]
                # print(client,system,job)
                # print(dfg)
                ok = False
                for perc in dfg['PERC']:
                    ok = perc==100
                    if not ok:
                        break
                l.append((ts,ok))
        # data[-1][-1] = sorted(data[-1][-1])
    # import pprint
    # pprint.pprint(data)
    return render_template('status.html', data=data)


if __name__ == '__main__':
    create_db('backup-log.db')

    parser = argparse.ArgumentParser()
    parser.add_argument('--no-catalogue', action='store_true', default=False, help='never catalogue e-mails in DB, just read DB')
    options = parser.parse_args()

    if not options.no_catalogue:
        worker = threading.Thread(target=handle_mails)
        worker.daemon = True
        worker.start()

    app.run(host='0.0.0.0', port=5009, debug=False)
