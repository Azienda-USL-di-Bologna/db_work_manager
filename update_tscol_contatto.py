#!/usr/bin/python3
# -*- coding: utf-8 -*-

import select
import psycopg2.extensions
import time
import datetime
import logging
import io
import sys
import traceback
import connessioni_db as db
import os.path

file_name_last_update_tscol = "last_update_tscol.json"

logging.basicConfig(
    filename='update_tscol_contatto.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
log = logging.getLogger("update_tscol_contatto")

q_update_tscol = "select rubrica.update_tscol(%s)"
q_update_tscol_loop_with_date_control = """
    DO $$ DECLARE contact integer;
    BEGIN 
       FOR contact IN SELECT id FROM rubrica.contatti WHERE version > %s
       LOOP
          PERFORM rubrica.update_tscol(contact);
       END LOOP;
    END; $$
"""
q_update_tscol_loop_without_date_control = """
    DO $$ DECLARE contact integer;
    BEGIN 
       FOR contact IN SELECT id FROM rubrica.contatti
       LOOP
          PERFORM rubrica.update_tscol(contact);
       END LOOP;
    END; $$
"""


def take_lock(conn):
    q = "select pg_advisory_lock(101010177)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def try_lock(conn):
    q = "select pg_try_advisory_lock(101010177)"
    c = conn.cursor()
    c.execute(q)
    return c.fetchone()[0]


def search_for_old_tscol(conn):
    c = conn.cursor()
    # Prendo la data dal file. Se il file non c'è faccio l'update su tutte le tscol
    if os.path.isfile(file_name_last_update_tscol):
        with open(file_name_last_update_tscol, "r") as fin:
            my_date = datetime.datetime.strptime(fin.read(), '%Y-%m-%d %H:%M:%S')
            log.info("La data letta è: %s" % my_date)
            c.execute(q_update_tscol_loop_with_date_control, (my_date,))
    else:
        # Il file non esiste, eseguo l'update tscol ovunque
        log.info("Il file non esiste, faccio l'update su tutti i contatti")
        c.execute(q_update_tscol_loop_without_date_control)
    c.close()
    update_file_of_last_update_tscol()


def update_file_of_last_update_tscol():
    f = open(file_name_last_update_tscol, "w")
    f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    f.close()


def update_tscol(conn, id_contatto):
    log.info("Faccio l'update del contatto con id: %s" %id_contatto)
    c = conn.cursor()
    c.execute(q_update_tscol, (id_contatto,))
    c.close()
    update_file_of_last_update_tscol()


if __name__ == '__main__':
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

            log.info("Guardo se ci sono dei contatti su cui non ho fatto l'update tscol")
            search_for_old_tscol(conn)

            curs = conn.cursor()
            curs.execute("LISTEN update_tscol_contatto;")

            log.info("In ascolto per altre richieste..")

            while 1:
                if select.select([conn], [], []):
                    log.info("Ricevuta notifica, devo fare un update")
                    conn.poll()
                    while conn.notifies:
                        notify = conn.notifies.pop(0)
                        update_tscol(conn, notify.payload)
        except:
            log.error("Probabile errore nella connessione al db o nell'esecuzione del lavoro.")
            output = io.StringIO()
            traceback.print_exception(*sys.exc_info(), limit=None, file=output)
            log.error(output.getvalue())
            if conn:
                conn.close()
            time.sleep(10)
