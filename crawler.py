#!/usr/bin/env python
import os
import sys
import ssl
import time
import socket
import random
import httplib
import logging
import urlnorm
import urllib2
import urlparse
import sqlite3
import logging.handlers
import MySQLdb
from BeautifulSoup import BeautifulSoup

try:
	connection = MySQLdb.connect (host = "localhost", user = "crawler", passwd = "cr4wl3r", db = "crawler")

except mysql_exceptions.OperationalError:
	logging.error("Unable to connect to database")
	sys.exit(1)

cursor = connection.cursor()
cursor2 = connection.cursor()

def extractlinks(html):

	links = []

	try:
		soup = BeautifulSoup(html)
		anchors = soup.findAll('a')

		for a in anchors:
			links.append(a['href'])
	except: pass

	return links

def rel2abs(resultset, url):

	s = []

	for link in resultset:
		if not link.lower().startswith("http"):
			s.append(urlparse.urljoin(url,link))

		else:
			s.append(link)

	return s

def insertIntoDB(links):

	global cursor2

	if len(links) > 0:
		for link in links:

			# We throw away fragments
			try:
				portions = urlnorm.parse(link)

			# We should either try to handle this and/or have a db for bad links
			except UnicodeEncodeError:
				continue

			link = urlparse.urlunsplit((portions[0], portions[1], portions[2], portions[3], ""))
			cursor2.execute("SELECT url FROM data WHERE url=%s", (link))
			row = cursor2.fetchone()
			crawlerID = random.randrange(0,100)

			if row == None:
				cursor2.execute("INSERT INTO data (url, timestamp_added, crawler_id) VALUES (%s, %s, %s)", (link, time.time(), crawlerID))


if __name__ == "__main__":

	crawlerID = "0"

	if (len(sys.argv) > 1):
		crawlerID = sys.argv[1]

	filename = "crawler" + crawlerID + ".log"

	logging.basicConfig(filename=os.path.join(filename), level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
	
	while (True):

		cursor.execute("SELECT url FROM data WHERE downloaded=0 AND crawler_id = %s", (crawlerID,))

		for row in cursor:

			try:
				logging.info("Downloading from " + row[0])

				if sys.version_info < (2, 6):
					f = urllib2.urlopen(row[0])

				# A timeout of 30 seconds is more then enough for anybody
				else:
					f = urllib2.urlopen(row[0], None, 30)

				contents = f.read().strip()

			except socket.error, e:
				logging.error("Connection reset by peer when reading data from " + row[0])
				continue


			except ssl.SSLError, e:
				cursor.execute("UPDATE data SET downloaded = 1, timestamp_downloaded = %s, http_code=-6 WHERE url = %s", (time.time(), row[0]))
				logging.error("Read SSL data timed out")
				continue


			except httplib.IncompleteRead, e:
				logging.error("Unable to read data completely from " + row[0] + ", passing over")
				continue


			except ValueError, e:
				cursor.execute("UPDATE data SET downloaded = 1, timestamp_downloaded = %s, http_code=-4 WHERE url = %s", (time.time(), row[0]))
				logging.error("Unable to read data from " + row[0])
				continue

			except socket.timeout, e:
				cursor.execute("UPDATE data SET downloaded = 1, timestamp_downloaded = %s, http_code=-3 WHERE url = %s", (time.time(), row[0]))
				logging.error("Timed out reading data from " + row[0])
				continue


			except httplib.BadStatusLine, e:
				cursor.execute("UPDATE data SET downloaded = 1, timestamp_downloaded = %s, http_code=-2 WHERE url = %s", (time.time(), row[0]))
				logging.error("Unable to read data and get status from " + row[0])
				continue


			except urllib2.URLError, e:
				cursor.execute("UPDATE data SET downloaded = 1, timestamp_downloaded = %s, http_code=-1 WHERE url = %s", (time.time(), row[0]))
				logging.error("Unable to read data from " + row[0])
				continue

			except urllib2.HTTPError, e:
				cursor.execute("UPDATE data SET downloaded = 1, timestamp_downloaded = %s, http_code=-1 WHERE url = %s", (e.code, time.time(), row[0]))
				logging.error("Unable to read data from " + row[0])
				continue

			links = extractlinks(contents)
			links = rel2abs(links, row[0])
			logging.info("Found " + str(len(links)) + " links in " + row[0])

			try:
				insertIntoDB(links)

			# We get silly key errors for u'\xeb'
			# so we just skip
			except KeyError:
				cursor.execute("UPDATE data SET downloaded = 1, timestamp_downloaded = %s, http_code=-5 WHERE url = %s", (time.time(), row[0]))
				logging.error("Unable to read data from " + row[0])
				continue

			

			cursor.execute("UPDATE data SET contents = %s, downloaded = 1, timestamp_downloaded = %s, http_code=200 WHERE url = %s", (contents, time.time(), row[0]))
			cursor.execute("COMMIT")

		# We sleep for a while to let other crawlers
		# populate database with possible URL's
		# This is a nasty hack, but then, isn't this
		# entire project a nasty hack? :)
		time.sleep(10)
