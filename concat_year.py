import os
months = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
          'August', 'September', 'October', 'November', 'December']

year = '2008'
 
fout=open("Merged{}.csv".format(year),"a")
header_saved = False
for month in months:	
	files = os.listdir('{}{}/'.format(month, year))
	
	try:
		files.remove('.DS_Store')
	except:
		pass
		
	for file in files:
		with open('{}{}/'.format(month, year)+file) as f:
			header = next(f)
			if not header_saved:
				fout.write(header)
				header_saved = True
			for line in f:
				fout.write(line)
fout.close()
