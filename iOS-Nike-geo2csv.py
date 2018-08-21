import sqlite3
import csv

#open database
db = sqlite3.connect('activityStore.db')
cursor = db.cursor()
cursor1 = db.cursor()
cursor2 = db.cursor()
#iterate to see which runs have lat and long
cursor.execute('''
select distinct sourceID from metrics
where type = 'latitude'
order by sourceID 
''')
rows = cursor.fetchall()
for row in rows:
	runid = (row[0])
	#print(runid)
	#select the first row of every run to extract the date and time. Those will be used to name the csv.
	cursor1.execute("SELECT lat.sourceid, datetime(lat.startDateInUtcSeconds, 'unixepoch', 'localtime') as day, lat.startDateInUtcSeconds, lat.value as latitude, long.value as long FROM metrics as lat left join metrics as long on lat.startDateInUtcSeconds = long.startDateInUtcSeconds WHERE lat.type = 'latitude' and long.type = 'longitude' and lat.sourceID = ? ORDER BY lat.startDateInUtcSeconds LIMIT 1", (runid,))
	rows1 = cursor1.fetchall()
	for row in rows1:
		rundate = row[1]
		#print(rundate)
		titlefile = rundate.replace(":", "-")
	#write the csv
	cursor.execute("SELECT lat.sourceid, datetime(lat.startDateInUtcSeconds, 'unixepoch', 'localtime') as day, lat.startDateInUtcSeconds, lat.value as latitude, long.value as long FROM metrics as lat left join metrics as long on lat.startDateInUtcSeconds = long.startDateInUtcSeconds WHERE lat.type = 'latitude' and long.type = 'longitude' and lat.sourceID = ? ORDER BY lat.startDateInUtcSeconds", (runid,))
	rows = cursor.fetchall()
	csvWriter = csv.writer(open(titlefile +'.csv', 'w', newline=''))
	for row in rows:
		#line =(row)
		#print(row)
		csvWriter.writerow(row)
