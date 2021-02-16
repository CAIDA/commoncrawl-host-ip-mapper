#!/usr/bin/env python3

import gzip
import datetime

domain_ip_map = {}
with gzip.open("common-crawl-2020-nov.csv.gz") as in_file:
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

with open("common-crawl-2020-nov.unique.csv", "wt") as out_file:
    for domain, (_, ip) in domain_ip_map.items():
        out_file.write(f"{domain},{ip}\n")
