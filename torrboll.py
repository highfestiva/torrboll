#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from base64 import b64decode as base64decode
from bs4 import BeautifulSoup
from collections import namedtuple
from datetime import datetime, timedelta
import dateutil.parser
from email import policy
from email.mime.text import MIMEText
from email.parser import BytesParser
from flask import Flask, render_template, request, send_from_directory
import imaplib
import pandas as pd
import quopri
import re
from smtplib import SMTP
from socket import gethostname
import sqlite3
import threading
import time
import traceback


app = Flask(__name__)
ImapCapabilities = namedtuple('ImapCapabilities', ['MOVE', 'UIDPLUS'])
clear_date = re.compile(r'(\(\d+/\d+/\d+\)|\d+/\d+/\d+)')


@app.route('/favicon.ico')
def favicon():
    return send_from_directory('static', 'favicon.ico', mimetype='image/x-icon')


@app.route('/status')
def status():
    days = int(request.args.get('days', 40))
    assert days>0
    data = get_table("WHERE TIMESTAMP >= DATE('NOW', '-%i DAYS')" % days)
    return render_template('status.html', data=data)


def imap_moverange(imap_conn, imap_features, target, msg_rng):
    # print('MOVE', msg_rng)
    if imap_features.MOVE:
        ok, data = imap_conn.uid('MOVE', '%s %s' % (msg_rng, target))
        if ok != 'OK':
            raise IOError('Cannot move messages to folder %s' % target)
    elif imap_features.UIDPLUS:
        ok, data = imap_conn.uid('COPY', '%s %s' % (msg_rng, target))
        if ok != 'OK':
            raise IOError('Cannot copy messages to folder %s' % target)
        ok, data = imap_conn.uid('STORE',
                                r'+FLAGS.SILENT (\DELETED) %s' % msg_rng)
        if ok != 'OK':
            raise IOError('Cannot delete messages.')
        ok, data = imap_conn.uid('EXPUNGE', msg_rng)
        if ok != 'OK':
            raise IOError('Cannot expunge messages.')
    else:
        ok, data = imap_conn.uid('COPY', '%s %s' % (msg_rng, target))
        if ok != 'OK':
            raise IOError('Cannot copy messages to folder %s' % target)
        ok, data = imap_conn.uid('STORE',
                                r'+FLAGS.SILENT (\DELETED) %s' % msg_rng)
        if ok != 'OK':
            raise IOError('Cannot delete messages.')


def imap_moveall(imap_conn, imap_features, target, messages):
    messages = [int(m) for m in messages]
    while messages:
        start = end = messages[0]
        messages = messages[1:]
        while messages:
            if messages[0] == end+1:
                end = messages[0]
                messages = messages[1:]
            else:
                break
        msg_rng = ('%i:%i'%(start,end)) if end>start else ('%i'%start)
        imap_moverange(imap_conn, imap_features, target, msg_rng)


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
    host,port,user,passwd = read_settings('imap-server.cfg')
    imap_conn = imaplib.IMAP4_SSL(host,int(port))
    imap_conn.login(user, passwd)
    ok,cap = imap_conn.capability()
    capas = cap[0].decode()
    imap_features = ImapCapabilities('MOVE' in capas, 'UIDPLUS' in capas)
    imap_conn.select()
    return imap_conn, imap_features


def imap_disconnect(imap_conn):
    imap_conn.close()
    imap_conn.logout()


def split_subject(subject):
    splitstr = '-' if subject.count('-')==2 else ' - '
    words = [e.strip() for e in subject.split(splitstr)]
    if len(words) >= 3:
        job,client,our_company = words[:3]
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
        if 'hrs' not in tds[4].text and 'mins' not in tds[4].text:
            perc = 0
        yield system, system, system, perc


def parse_storage_craft(subject, html):
    subject = subject.split('Online Image Report:')[-1].strip()
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
    job,client = split_subject(subject)
    phtml = BeautifulSoup(html, features='html.parser')
    for system_row in phtml.find_all('table', width='100%'):
        system = '?'
        perc = 0
        for td in system_row.find_all('td'):
            system = td.text.strip()
            system = system.split('Backupset:')[-1].strip()
            for span in system_row.find_all('span'):
                if 'SUCCESS' in span.text:
                    perc = 100
                break
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
    processed = False
    if 'Code42' in subject and 'Backup Report' in subject:
        service = 'CrashPlan PRO'
        result = parse_crashplan(subject, html)
        processed = True
    elif 'Online Image Report' in subject:
        service = 'Storage Craft'
        result = parse_storage_craft(subject, html)
        processed = True
    elif 'Backup Summary:' in subject:
        service = 'Ahsay'
        result = parse_ahsay(subject, html)
        processed = True
    elif 'Hyper-V Server Report' in subject:
        service = 'Hyper-V'
        result = parse_hyper_v(subject, html)
        processed = True
    else:
        print('Junk mail?', subject)
    timestamp = timestamp2utc(timestamp)
    for client,system,job,perc in result:
        s = "INSERT OR REPLACE INTO BACKUP_LOG (TIMESTAMP, SERVICE, CLIENT, SYSTEM, JOB, PERC) values (DATETIME('%s'), '%s', '%s', '%s', '%s', %i);" % (timestamp, service, client, system, job, perc)
        cursor.execute(s)
    return processed


def parse_all(imap_conn, imap_features):
    print('Parsing e-mails...')
    db = sqlite3.connect('backup-log.db')
    cursor = db.cursor()
    _,mail_results = imap_conn.search(None, 'ALL')
    _,uid_results = imap_conn.uid('search', None, 'ALL')
    msg_ids = mail_results[0].split()
    msg_uids = uid_results[0].split()
    parsed_uids = []
    for msgid,uid in zip(msg_ids, msg_uids):
        subject = timestamp = ''
        try:
            _,data = imap_conn.fetch(msgid, '(RFC822)')
            msg = BytesParser(policy=policy.default).parsebytes(data[0][1])
            subject = msg['Subject']
            subject = clear_date.sub('', subject).strip()
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
            if catalogue_mail(cursor=cursor, subject=subject, timestamp=timestamp, html=html):
                parsed_uids += [uid]
        except:
            print('ERROR MSGID, SUBJECT & TIMESTAMP:', msgid, subject, timestamp)
            traceback.print_exc()
    if parsed_uids:
        cursor.execute('COMMIT')
        imap_moveall(imap_conn, imap_features, '/Processed', parsed_uids)
    print('%i e-mails catalogued.' % len(parsed_uids))


def timestamp2utc(t):
    return datetime.utcfromtimestamp(t).isoformat() + 'Z'


def handle_mails():
    while True:
        target = datetime.now().replace(hour=11, minute=0, second=0, microsecond=0)
        if target < datetime.now():
            target += timedelta(hours=24)
        seconds = (target - datetime.now()).total_seconds()
        print('Sleeping %i:%2.2i...' % (seconds//60//60, seconds//60%60))
        time.sleep(seconds)

        try:
            imap_conn,imap_features = imap_connect()
            parse_all(imap_conn, imap_features)
            imap_disconnect(imap_conn)

            summarize()
        except Exception as e:
            traceback.print_exc()


def summarize():
    data = get_table("WHERE TIMESTAMP >= DATE('NOW', '-3 DAYS')")
    bad_boys = []
    for service,service_data in data:
        for client,system,job,infolist in service_data:
            _,_,ok = infolist[-1]
            if not ok:
                bad_boys.append('%s: %s, %s (job %s)' % (service,client,system,job))
    if bad_boys:
        print('Placing ticket on %i failed backups.' % len(bad_boys))
        url = 'http://%s:5009/status' % gethostname()
        msg = 'Failed backups:\n\n' + '\n'.join(bad_boys) + ('\n\nMore info here: %s\n' % url)
        msg = MIMEText(msg, 'plain')
        msg['Subject'] = 'Backup failure'
        host,port,user,passwd = read_settings('smtp-server.cfg')
        msg['From'] = user
        receivers = [l.strip() for l in open('receivers.cfg')]
        smtp = SMTP(host, port)
        smtp.ehlo()
        smtp.starttls()
        smtp.login(user, passwd)
        smtp.sendmail(user, receivers, msg.as_string())
        smtp.quit()
        print('Ticket placed.')


def get_table(where):
    db = sqlite3.connect('backup-log.db')
    sql = 'SELECT * FROM BACKUP_LOG %s ORDER BY TIMESTAMP' % where
    df = pd.read_sql_query(sql, db)
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP']).dt.date
    data = []
    cache = {}
    for service,dfs in df.groupby('SERVICE'):
        data += [[service, []]]
        dft = pd.pivot_table(dfs, values='PERC', index=['CLIENT','SYSTEM','JOB'], columns='TIMESTAMP')
        for timestamp in dft.columns:
            ts = str(timestamp).split(' ')[0]
            tt = '-'.join(ts.split('-')[:-1]) if ts.endswith('01') else ''
            ts = ts.split('-')[-1]
            for client,system,job in dft.index:
                if (service,client,system,job) not in cache:
                    l = cache[(service,client,system,job)] = []
                    data[-1][-1].append((client,system,job, l))
                else:
                    l = cache[(service,client,system,job)]
                # print(client,system,job)
                # print(dfg)
                ok = False
                perc = dft.loc[(client,system,job), timestamp]
                ok = perc==100
                l.append((tt,ts,ok))
        # data[-1][-1] = sorted(data[-1][-1])
    # import pprint
    # pprint.pprint(data)
    return data


def read_settings(fname):
    server = open(fname).read().strip()
    u_p,h_p = server.rsplit('@',1)
    host,port = h_p.rsplit(':',1)
    user,passwd = u_p.split(':',1)
    return host,int(port),user,passwd


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
