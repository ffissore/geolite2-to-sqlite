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

c.execute("DROP TABLE IF EXISTS locations")
c.execute("CREATE TABLE locations (geoname_id int primary key, continent_code varchar(255), continent_name varchar(255), country_code varchar(255), country_name varchar(255), region varchar(255), province varchar(255), city varchar(255))")

with open("GeoLite2-City-Locations-en.csv", "r") as csvfile:
    rows = csv.reader(csvfile)
    first = True
    for row in rows:
        if first:
            first = False
            continue
        if len(row) != 13:
            raise Exception("row " + str(row) + " is not made of 13 columns")
        
        c.execute("INSERT INTO locations VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (row[0], row[2], row[3], row[4], row[5], row[7], row[9], row[10]))

conn.commit()

conn.close()

