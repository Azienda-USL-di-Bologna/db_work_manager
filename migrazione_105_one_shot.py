#!/usr/bin/python3
# -*- coding: utf-8 -*-

import psycopg2.extensions
import time
import logging
import sys
from datetime import timedelta
import datetime

logging.basicConfig(
    filename='migrazione_105_one_shot.log',
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p'
)
log = logging.getLogger("migrazione_105_one_shot")
consoleHandler = logging.StreamHandler(sys.stdout)
log.addHandler(consoleHandler)

if __name__=='__main__':
    log.info("Ok parto")
    conn = psycopg2.connect(
        user="foreign_role",
        password="********",
        host="toshiro",
        database="argo"
    )
    log.info("Mi sono connesso a toshiro/argo come foreign_role")

    cur = conn.cursor()
    oggi = datetime.datetime.now().date()
    q = """
        select * from babel.migrazione_attivita_fatte_from_locale_to_internauta(%s, %s);
    """
    # Data di partenza: '2015-09-01'
    data_dal =  datetime.datetime(2015, 9, 1).date()
    ho_ancora_da_fare = True
    data_al = None

    while ho_ancora_da_fare:
        data_al = data_dal
        data_al += datetime.timedelta(days=1)
        log.info("Chiamo la store procedure con: " + "Data dal: %s" % data_dal.isoformat() + " e: " + "Data al: %s" % data_al.isoformat())
        # cur.execute(q, (data_dal.isoformat(),  data_al.isoformat()))
        log.info("Committo")
        # cur.commit()
        log.info("Sposto la data dal un giorno più avanti")
        data_dal = data_al
        if data_dal > oggi:
            ho_ancora_da_fare = False
        log.info("Dormo 5 secondi prima di ricominciare")
        time.sleep(5)