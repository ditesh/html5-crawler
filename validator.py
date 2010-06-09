import os
import sys
import time
import sqlite3
import logging
import subprocess
import logging.handlers

connection = sqlite3.connect('crawler.db')
cursor = connection.cursor()

if __name__ == "__main__":

	logging.basicConfig(filename=os.path.join("validator.log"), level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

	while (True):

		cursor.execute("SELECT url, contents FROM data WHERE downloaded='t' AND http_code=200 AND tidy_parsed='f'")

		for row in cursor:

			errors = 0
			warnings = 0

			try:
				logging.info("Tidying " + row[0])
				process = subprocess.Popen(["/usr/bin/tidy", "-eq"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
				process.stdin.write(row[1])
				process.stdin.close()

				result=process.stderr.read().split("\n")

				for line in result:
					if "Warning" in line:
						warnings += 1

					elif "Error" in line:
						errors += 1

			except OSError:
				cursor.execute("UPDATE data SET tidy_parsed = 't', tidy_warnings = -1, tidy_errors = -1, timestamp_tidy_parsed = ? WHERE url = ?", (time.time(), row[0]))
				logging.error("Unable to tidy " + row[0])
				continue

			cursor.execute("UPDATE data SET tidy_parsed = 't', tidy_warnings = ?, tidy_errors = ?, timestamp_tidy_parsed = ? WHERE url = ?", (warnings, errors, time.time(), row[0]))
			connection.commit()
			continue
