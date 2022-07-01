#!/usr/bin/python3
# -*- coding: utf-8 -*-

import select
import psycopg2.extensions
from psycopg2.extras import Json
import time
import logging
import io
import sys
import traceback
import connessioni_db as db

logging.basicConfig(
    filename='worker.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
log = logging.getLogger("worker")

q = "SELECT id, row_data, target_schema, target_table, where_condition, operation from tools.to_do ORDER BY id asc LIMIT 100"
qWorker = "select tools.work_manager(%s, %s, %s, %s, %s, %s)"


def take_lock(conn):
    q = "select pg_advisory_lock(4815162342)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def try_lock(conn):
    q = "select pg_try_advisory_lock(4815162342)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def search_and_work(conn):
    curs = conn.cursor()
    curs.execute(q)
    while curs.rowcount != 0:
        rows = curs.fetchall()
        for r in rows:
            try:
                curs.execute(qWorker, (r[0], Json(r[1]), r[2], r[3], r[4], r[5]))
            except:
                log.error("Errore nell'esecuzione del lavoro.")
                output = io.StringIO()
                traceback.print_exception(*sys.exc_info(), limit=None, file=output)
                log.error(output.getvalue())
                raise
        curs.execute(q)
    curs.close()


if __name__=='__main__':
    while True:
        try:
            conn = psycopg2.connect(
                user=db.connessioni_db['user'],
                password=db.connessioni_db['password'],
                host=db.connessioni_db['host'],
                database=db.connessioni_db['db']
            )
            log.info("Connessione al db stabilita")

            if not try_lock(conn):
                log.info("Un'altra istanza dello script e' in esecuzione. Esco.")
                sys.exit(0)
            else:
                take_lock(conn)

            # Autocommit obbligatorio per stare in ascolto di notify.
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            log.info("Guardo se c'è del lavoro da fare subito")
            search_and_work(conn)

            curs = conn.cursor()
            curs.execute("LISTEN other_work;")

            log.info("In ascolto per altro lavoro..")

            while 1:
                if select.select([conn], [], []):
                    log.info("Ricevuta notifica di altro lavoro")
                    conn.poll()
                    while conn.notifies:
                        del conn.notifies[:]
                        search_and_work(conn)
        except:
            log.error("Probabile errore nella connessione al db o nell'esecuzione del lavoro.")
            output = io.StringIO()
            traceback.print_exception(*sys.exc_info(), limit=None, file=output)
            log.error(output.getvalue())
            time.sleep(10)
