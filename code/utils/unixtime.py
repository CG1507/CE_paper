import datetime

def get_date(date_time):
	"""
	It will return date from the date and time %Y-%m-%d %H:%M:%S format.
	
	Args:
		date_time (string): Date and time in %Y-%m-%d %H:%M:%S format 

	Returns:
		date (string): Date from the %Y-%m-%d %H:%M:%S format
	"""
	return date_time.split()[0].split('-')[2]

def get_month(date_time):
	"""
	It will return month from the date and time %Y-%m-%d %H:%M:%S format.
	
	Args:
		date_time (string): Date and time in %Y-%m-%d %H:%M:%S format 

	Returns:
		month (string): Month from the %Y-%m-%d %H:%M:%S format
	"""
	return date_time.split()[0].split('-')[1]

def get_year(date_time):
	"""
	It will return year from the date and time %Y-%m-%d %H:%M:%S format.
	
	Args:
		date_time (string): Date and time in %Y-%m-%d %H:%M:%S format 

	Returns:
		year (string): Year from the %Y-%m-%d %H:%M:%S format
	"""
	return date_time.split()[0].split('-')[0]

def get_hour(date_time):
	"""
	It will return hour from the date and time %Y-%m-%d %H:%M:%S format.
	
	Args:
		date_time (string): Date and time in %Y-%m-%d %H:%M:%S format 

	Returns:
		hour (string): Hour from the %Y-%m-%d %H:%M:%S format
	"""
	return date_time.split()[1].split(':')[0]

def get_minute(date_time):
	"""
	It will return minute from the date and time %Y-%m-%d %H:%M:%S format.
	
	Args:
		date_time (string): Date and time in %Y-%m-%d %H:%M:%S format 

	Returns:
		minute (string): Minute from the %Y-%m-%d %H:%M:%S format
	"""
	return date_time.split()[1].split(':')[1]

def get_second(date_time):
	"""
	It will return seconds from the date and time %Y-%m-%d %H:%M:%S format.
	
	Args:
		date_time (string): Date and time in %Y-%m-%d %H:%M:%S format 

	Returns:
		seconds (string): Seconds from the %Y-%m-%d %H:%M:%S format
	"""
	return date_time.split()[1].split(':')[2]

def get_day(date_time):
	"""
	It will return day by using the date and time %Y-%m-%d %H:%M:%S format.
	
	Args:
		date_time (string): Date and time in %Y-%m-%d %H:%M:%S format 

	Returns:
		month (int): Day from the %Y-%m-%d %H:%M:%S format
					 Monday is 1 and Sunday is 7
	"""
	year = get_year(date_time)
	month = get_month(date_time)
	date = get_date(date_time)

	return datetime.date(int(year), int(month), int(date)).isoweekday()

def convert(timestamp):
	"""
	Convert timestamp in date-time format.

	Args:
		timestamp:
	Returns:
		date_time:
	"""
	date_time = datetime.datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
	return str(date_time)

def days_difference(date0, date1):
	d0 = datetime.date(date0[0], date0[1], date0[2])
	d1 = datetime.date(date1[0], date1[1], date1[2])
	diff = d1 - d0
	return diff.days

def test():
	"""
	Test function of unixtime.
	"""
	timestamp = "1535980894"
	date_time = convert(timestamp)
	print(date_time)
	print('Day', get_day(date_time))
	print('Month', get_month(date_time))
	print('year', get_year(date_time))
	print('date', get_date(date_time))
	print('Hour', get_hour(date_time))
	print('Minute', get_minute(date_time))
	print('Seconds', get_second(date_time))
	print(days_difference([2008, 1, 10], [2009, 1, 10]))

if __name__ == '__main__':
	test()