#!/usr/bin/env python3
from dotenv import load_dotenv, find_dotenv
import os
import psycopg2
import gzip


def main():
    # load credentials
    load_dotenv(find_dotenv(".reverse-dns-db-cred"), override=True)
    cred = {
        'user': os.environ.get("REVERSE_DNS_DB_USER", None),
        'password': os.environ.get("REVERSE_DNS_DB_PASSWORD", None),
        'port': os.environ.get("REVERSE_DNS_DB_PORT", None),
        'host': os.environ.get("REVERSE_DNS_DB_HOST", None),
    }

    conn = psycopg2.connect(**cred)
    cur = conn.cursor()
    f = open('common-crawl-2020-nov.unique.csv')
    cur.copy_from(f, 'common_crawl', sep=",")
    conn.commit()
    conn.close()

if __name__=="__main__":
    main()
