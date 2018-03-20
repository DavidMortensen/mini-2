import sys
import os
import time
from collections import OrderedDict


def query1(files, name_file):

	files = [file for file in os.listdir(files) if file.endswith(".csv")]

	timeStart = time.time()

	Sum = float()

	with open(name_file, 'r') as f2:
		for line in f2:
			line=line.strip('\n')
			newline=line.split(',')

	for file in files:
		with open(sys.argv[1]+'/'+file, "r") as f:
			header = f.readline()
			for line in f:
				line=line.strip('\n')
				newline=line.split(',')

				try:
					payment = float(newline[13])
					Sum += payment
				except ValueError:
					continue


	print('QUERY 1')
	print('The sum of the total payments is:', "%.2f" %Sum)
	print('time:', time.time() - timeStart)

def query2(files, name_file):

	files = [file for file in os.listdir(files) if file.endswith(".csv")]

	timeStart = time.time()

	d = {}

	with open(name_file, 'r') as f2:
		for line in f2:
			line=line.strip('\n')
			newline=line.split(',')

	for file in files:
		with open(sys.argv[1]+'/'+file, "r") as f:
			header = f.readline()
			for line in f:
				line=line.strip('\n')
				newline=line.split(',')

				try:
					key = int(newline[15])
					value = float(newline[13])
					
				except ValueError:
					continue

				if key in d:
					d[key].append(value)
				else:
					d[key] = [value]

	od = OrderedDict(sorted(d.items()))

	sum_d = {}
	for key, value in od.items():
		try:
			sum_d[key] = sum(map(float, value))
			sum_d[key] = round(sum_d[key],4)
		except ValueError:
			continue

	print('QUERY 2')
	print(sum_d)
	#print(sum(sum_d.values()))
	print('time:', time.time() - timeStart)


def query3(files, name_file):

	files = [file for file in os.listdir(files) if file.endswith(".csv")]

	timeStart = time.time()

	Sum = float()

	with open(name_file, 'r') as f2:
		for line in f2:
			line=line.strip('\n')
			newline=line.split(',')

	for file in files:
		with open(sys.argv[1]+'/'+file, "r") as f:
			header = f.readline()
			for line in f:
				line=line.strip('\n')
				newline=line.split(',')

				if newline[14] == 'Cash':
					try:
						payment = float(newline[13])
						Sum += payment
					except ValueError:
						continue

	print('QUERY 3')
	print('The sum of the total payments by cash is:', "%.2f" %Sum)
	print('time:', time.time() - timeStart)
				
def query4(files, name_file):

	files = [file for file in os.listdir(files) if file.endswith(".csv")]

	timeStart = time.time()
				
	name_list = set()

	for file in files:
		with open(sys.argv[1]+'/'+file, "r") as f:
			header = f.readline()
			for line in f:
				line=line.strip('\n')
				newline=line.split(',')

				if newline[15]=='11':
					try:
						with open(name_file, 'r') as f2:
							for line in f2:
								line=line.strip('\n')
								nameline=line.split(',')
					
								if newline[0]==nameline[0]:
									name_list.add(nameline[1].strip('\r'))
					except:
						pass
				else:
					pass


	print('QUERY 4')
	print(name_list)
	print('number of different names:', len(name_list))
	print('time:', time.time() - timeStart)


if __name__ == "__main__":

	files = sys.argv[1]
				
	name_file = sys.argv[2]

	Q = sys.argv[3]


	if Q == 'query1':
		query1(files, name_file)
	elif Q == 'query2':
		query2(files, name_file)
	elif Q == 'query3':
		query3(files, name_file)
	elif Q == 'query4':
		query4(files, name_file)
	else:
		print('please choose either query1, query2, query3 or query4')

