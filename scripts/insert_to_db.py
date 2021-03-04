#!/usr/bin/env python3
import argparse

from dotenv import load_dotenv, find_dotenv
import os
import psycopg2

import gzip
import datetime
import tempfile


class MappingCommitter:

    def __init__(self, table_name, input_file):
        self.table_name = table_name
        self.input_file = input_file
        self.temp_file = None
        self.conn = None

    def extract_unique_domain_ip_mapping(self):
        """
        Given a csv gz file, extract unique domain to ip pairs (use the most recent mapping), write to a temporary csv file
        and return the file name.

        :param csv_gz_file: the input compressed csv file
        :return: name of the uncompressed csv file containing unique domain-ip mapping
        """
        domain_ip_map = {}

        with gzip.open(self.input_file) as in_file:
            for line in in_file:
                domain, date, ip = line.decode().strip().split(",")
                if domain not in domain_ip_map:
                    domain_ip_map[domain] = (date, ip)
                else:
                    old_date, old_ip = domain_ip_map[domain]
                    if ip == old_ip:
                        continue
                    date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
                    old_date_obj = datetime.datetime.strptime(old_date, '%Y-%m-%d')
                    if date_obj > old_date_obj:
                        domain_ip_map[domain] = (date, ip)

        fh = tempfile.NamedTemporaryFile(delete=False, mode="w")
        for domain, (_, ip) in domain_ip_map.items():
            fh.write(f"{domain},{ip}\n")
        fh.flush()
        self.temp_file = fh.name

    def create_table(self):
        try:
            cur = self.conn.cursor()
            cur.execute(f"""
            CREATE TABLE {self.table_name}
        (
            domain character varying NOT NULL,
            ip inet NOT NULL,
            PRIMARY KEY (domain)
        )
            """)
        except psycopg2.errors.DuplicateTable as e:
            # table already exist, it's fine
            return
        finally:
            self.conn.commit()

    def create_conn(self):
        # load credentials
        load_dotenv(find_dotenv(".reverse-dns-db-cred"), override=True)
        cred = {
            'user': os.environ.get("REVERSE_DNS_DB_USER", None),
            'password': os.environ.get("REVERSE_DNS_DB_PASSWORD", None),
            'port': os.environ.get("REVERSE_DNS_DB_PORT", None),
            'host': os.environ.get("REVERSE_DNS_DB_HOST", None),
        }

        self.conn = psycopg2.connect(**cred)

    def upload_mapping(self, delete=False):
        cur = self.conn.cursor()

        cur.execute(f"delete from {self.table_name}")
        self.conn.commit()

        f = open(self.temp_file)
        cur.copy_from(f, self.table_name, sep=",")
        self.conn.commit()
        f.close()

        if delete:
            os.remove(self.temp_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
    Commit common_crawl mapping to database
    """)

    parser.add_argument('-t', '--table-name',
                        nargs='?', required=False,
                        help='table name')

    parser.add_argument('-i', '--input-file',
                        nargs='?', required=True,
                        help='Input file name')

    opts = vars(parser.parse_args())

    print(opts)

    input_file = opts["input_file"]
    assert input_file.endswith(".gz")

    table_name = opts["table_name"]
    if not table_name:
        assert input_file.split("/")[-1].startswith("mapping-")
        table_name = "_".join(input_file.split("/")[-1].split(".")[0].split("-")[1:])

    print(input_file, table_name)

    committer = MappingCommitter(table_name, input_file)

    committer.create_conn()
    committer.create_table()
    committer.extract_unique_domain_ip_mapping()
    committer.upload_mapping(delete=True)
