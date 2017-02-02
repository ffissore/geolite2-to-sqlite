# -*- coding: UTF-8 -*-
# Copyright (C) 2017 Federico Fissore <federico@fissore.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import csv
import sqlite3
import ipaddress

conn = sqlite3.connect("geoip.db")

c = conn.cursor()

c.execute("DROP TABLE IF EXISTS networks")
c.execute("CREATE TABLE networks (first_2_octects varchar(255), network varchar(255), geoname_id int, postalcode varchar(255), latitude double, longitude double, accurancy int)")

with open("GeoLite2-City-Blocks-IPv4.csv", "r") as csvfile:
    rows = csv.reader(csvfile)
    first = True
    for row in rows:
        if first:
            first = False
            continue
        if len(row) != 10:
            raise Exception("row " + str(row) + " is not made of 10 columns")
        if not row[1]:
            continue

        network = ipaddress.IPv4Network(row[0])
        if network.prefixlen >= 16:
            subnets = [network]
        else:
            subnets = list(network.subnets(16 - network.prefixlen))

        for subnet in subnets:
            first_2_octects = ".".join(str(subnet).split(".")[0:2])
            c.execute("INSERT INTO networks VALUES (?, ?, ?, ?, ?, ?, ?)", (first_2_octects, str(subnet), row[1], row[6], row[7], row[8], row[9]))

conn.commit()

c.execute("CREATE INDEX idx_first_2_octects ON networks (first_2_octects)")

conn.close()

