#!/usr/bin/env python
import sys
import time
import logging
import logging.handlers
import MySQLdb
from BeautifulSoup import BeautifulSoup

connection = MySQLdb.connect (host = "localhost", user = "crawler", passwd = "cr4wl3r", db = "crawler")
cursor = connection.cursor()


if __name__ == "__main__":

	previousLinks = 0
	previousDownloads = 0
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

	while True:

		cursor.execute("SELECT count(*) FROM data")
		row = cursor.fetchone()
		logging.info("Total links: " + str(row[0]))
		logging.info("Increase : " + str(row[0] - previousLinks))
		logging.info("Rate: " + str(round((row[0] - previousLinks)/60.0, 2)) + " links/second")
		previousLinks = row[0]
		print

		cursor.execute("SELECT count(*) FROM data WHERE downloaded=1")
		row = cursor.fetchone()
		logging.info("Total downloads: " + str(row[0]))
		logging.info("Increase : " + str(row[0] - previousDownloads))
		logging.info("Rate: " + str(round((row[0] - previousDownloads)/60.0, 2)) + " pages/second")
		previousDownloads = row[0]
		print

		time.sleep(60)
