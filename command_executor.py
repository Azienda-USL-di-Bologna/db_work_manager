#!/usr/bin/python3
# -*- coding: utf-8 -*-

import select
import psycopg2.extensions
# from psycopg2.extras import Json
import time
import logging
import io
import sys
import traceback
import connessioni_db as db
import os

logging.basicConfig(
    filename='command_executor.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
log = logging.getLogger("command_executor")


def take_lock(conn):
    q = "select pg_advisory_lock(73737373)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def try_lock(conn):
    q = "select pg_try_advisory_lock(73737373)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def search_and_work(conn):
    curs = conn.cursor()
    q = "SELECT command, parameters FROM tools.commands WHERE to_perform = true"
    curs.execute(q)
    rows = curs.fetchall()
    for r in rows:
        try:
            if r[0] == "RESTART_SHPECK":
                log.info("Riavvio lo shpeck")

                status = os.system(db.prefix_to_shpeck_server + "systemctl status shpeck_service.service")
                if status == 0:
                    os.system(db.prefix_to_shpeck_server + "systemctl restart shpeck_service.service")

                status = os.system(db.prefix_to_shpeck_server + "systemctl status shpeck_riconciliazione.service")
                if status == 0:
                    os.system(db.prefix_to_shpeck_server + "systemctl restart shpeck_riconciliazione.service")

                status = os.system(db.prefix_to_shpeck_server + "systemctl status shpeck_checker.service")
                if status == 0:
                    os.system(db.prefix_to_shpeck_server + "systemctl restart shpeck_checker.service")

                qu = "UPDATE tools.commands set to_perform = false WHERE command = 'RESTART_SHPECK'"
                curs.execute(qu)
                conn.commit()
        except:
            log.error("Errore nell'esecuzione del lavoro.")
            output = io.StringIO()
            traceback.print_exception(*sys.exc_info(), limit=None, file=output)
            log.error(output.getvalue())
            raise


if __name__=='__main__':
    while True:
        try:
            #conn = psycopg2.connect(
            #    user=db.connessioni_db['user'],
            #    password=db.connessioni_db['password'],
            #    host=db.connessioni_db['host'],
            #    database=db.connessioni_db['db']
            #)
            log.info("Connessione al db stabilita")

            #if not try_lock(conn):
            #    log.info("Un'altra istanza dello script e' in esecuzione. Esco.")
            #    sys.exit(0)
            #else:
            #    take_lock(conn)

            while 1:
                conn = psycopg2.connect(
                    user=db.connessioni_db['user'],
                    password=db.connessioni_db['password'],
                    host=db.connessioni_db['host'],
                    database=db.connessioni_db['db']
                )
                log.info("Connessione al db stabilita. Faccio un check")
                search_and_work(conn)
                conn.close()
                time.sleep(600)

        except:
            log.error("Probabile errore nella connessione al db o nell'esecuzione del lavoro.")
            output = io.StringIO()
            traceback.print_exception(*sys.exc_info(), limit=None, file=output)
            log.error(output.getvalue())
            time.sleep(10)