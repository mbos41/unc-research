import os

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
years = [2007, 2008, 2009]

for year in years:
	for month in months:
		if year == 2007:
			if month not in ['January', 'February', 'March']:
				os.makedirs('data copy/{}'.format(month+str(year)))
		elif year == 2009:
			if month not in ['July', 'August', 'September', 'October', 'November', 'December']:
				os.makedirs('data copy/{}'.format(month+str(year)))
		else:
			os.makedirs('data copy/{}'.format(month+str(year)))

				
