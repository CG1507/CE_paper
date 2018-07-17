import datetime

def get_date(date_time):
	"""
	
	Args:

	Returns:

	"""
	return date_time.split()[0].split('-')[2]

def get_month(date_time):
	"""
	
	Args:

	Returns:

	"""
	return date_time.split()[0].split('-')[1]

def get_year(date_time):
	"""
	
	Args:

	Returns:

	"""
	return date_time.split()[0].split('-')[0]

def get_hour(date_time):
	"""
	
	Args:

	Returns:

	"""
	return date_time.split()[1].split(':')[0]

def get_minute(date_time):
	"""
	
	Args:

	Returns:

	"""
	return date_time.split()[1].split(':')[1]

def get_second(date_time):
	"""
	
	Args:

	Returns:

	"""
	return date_time.split()[1].split(':')[2]

def get_day(date_time):
	"""
	Monday is 1 and Sunday is 7

	Args:

	Returns:

	"""
	year = get_year(date_time)
	month = get_month(date_time)
	date = get_date(date_time)

	return datetime.date(int(year), int(month), int(date)).isoweekday()

def convert(timestamp):
	date_time = datetime.datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
	return str(date_time)

def test():
	"""
	
	Args:

	Returns:

	"""
	timestamp = "1131750079"
	date_time = convert(timestamp)
	print(date_time)
	print('Day', get_day(date_time))
	print('Month', get_month(date_time))
	print('year', get_year(date_time))
	print('date', get_date(date_time))
	print('Hour', get_hour(date_time))
	print('Minute', get_minute(date_time))
	print('Seconds', get_second(date_time))

if __name__ == '__main__':
	test()