import sqlite3
import csv

#open database
db = sqlite3.connect('com.nike.activitystore.database')
cursor = db.cursor()
cursor1 = db.cursor()

#iterate to see which runs have lat and long
cursor.execute('''
select * from activity_metric_group 
where activity_metric_group.mg_metric_type = 'latitude' 
order by activity_metric_group.mg_activity_id 
''')

#save the run numbers, by inserting in a temp table, that have lat and long to be used for selection later in the view portion 
rows = cursor.fetchall()
for row in rows:
	runs =row[4]
	#print (runs)
	cursor1.execute('drop table if exists temp')
	cursor1.execute('create table if not exists temp(run integer)')
	#print (runs)
	cursor1.execute('insert into temp values (?)', (runs,))
	db.commit()
	#print (runs)
	
	cursor.execute('''
	drop view if exists sub1;
	''')
	#create a view that aggregates all data from the two pertinent tables
	cursor.execute('''
    create view sub1 
	as
	select * from activity_raw_metric, activity_metric_group, temp 
	where activity_raw_metric.rm_metric_group_id = activity_metric_group._id and activity_metric_group.mg_activity_id = temp.run 
	order by activity_raw_metric.rm_start_utc_millis; 
	''')
	#select the first row of all runs. Use the time and date for the csv filename.
	cursor.execute('''
	select datetime(lat.rm_start_utc_millis / 1000, 'unixepoch', 'localtime') as day, lat.rm_value as latitude, long.rm_value as longitude
	from sub1 as lat left join sub1 as long on lat.rm_start_utc_millis = long.rm_start_utc_millis
	where lat.mg_metric_type = 'latitude' and long.mg_metric_type = 'longitude'
	order by lat.rm_start_utc_millis LIMIT 1;
	''')
	
	rows = cursor.fetchall()
	for row in rows:
		rundate = row[0]
		titlefile = rundate.replace(":", "-")

	#Self join query of the view to aggregate lat and long in one row per corresponding point in time. Write csv file. 	
	cursor.execute('''
	select datetime(lat.rm_start_utc_millis / 1000, 'unixepoch', 'localtime') as day, lat.rm_value as latitude, long.rm_value as longitude
	from sub1 as lat left join sub1 as long on lat.rm_start_utc_millis = long.rm_start_utc_millis
	where lat.mg_metric_type = 'latitude' and long.mg_metric_type = 'longitude'
	order by lat.rm_start_utc_millis;
	''')
	
	rows = cursor.fetchall()
	csvWriter = csv.writer(open(titlefile +'.csv', 'w', newline=''))
	
	for row in rows:
		csvWriter.writerow(row)	

cursor.execute('''
	drop view if exists sub1;
	''')
	
cursor1.execute('drop table if exists temp')

