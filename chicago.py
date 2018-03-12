import sys
import os
import time
from collections import OrderedDict


def query1(all_lines):

	print(len(all_lines))

	timeStart = time.time()

	Sum = float()

	for line in all_lines:
		try:
			payment = float(line[13])
			Sum += payment
		except ValueError:
			continue


	print('QUERY 1')
	print('The sum of the total payments is:', "%.2f" %Sum)
	print('time:', time.time() - timeStart)

def query2(all_lines):

	print(len(all_lines))

	timeStart = time.time()

	d = {}

	for line in all_lines:
		try:
			key = int(line[15])
			value = float(line[13])
			
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
	print(sum(sum_d.values()))
	print('time:', time.time() - timeStart)


def query3(all_lines):

	print(len(all_lines))

	timeStart = time.time()

	Sum = float()

	for line in all_lines:
		if line[14] == 'Cash':
			try:
				payment = float(line[13])
				Sum += payment
			except ValueError:
				continue

	print('QUERY 3')
	print('The sum of the total payments by cash is:', "%.2f" %Sum)
	print('time:', time.time() - timeStart)
				
def query4(all_lines, names):
				
	name_list = set()

	print(len(all_lines))
	print(len(names))
	timeStart = time.time()

	for line in all_lines:
		if line[15]=='11':
			try:
				for name in names:
					if line[0]==name[0]:
						name_list.add(name[1])
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

	files = [file for file in os.listdir(files) if file.endswith(".csv")]
	all_lines = []

	start = time.time()

	for file in files:
		with open(sys.argv[1]+'/'+file, "r") as f:
			header = f.readline()
			for line in f:
				line=line.strip('\n')
				newline=line.split(',')
				all_lines.append(newline)


	name_file = sys.argv[2]

	names=[]
	with open(name_file, 'r') as f2:
		for line in f2:
			line=line.strip('\n')
			newline=line.split(',')
			names.append(newline)

	print('time for reading in files:', time.time() - start)

	Q = sys.argv[3]

	if Q == 'query1':
		query1(all_lines)
	elif Q == 'query2':
		query2(all_lines)
	elif Q == 'query3':
		query3(all_lines)
	elif Q == 'query4':
		query4(all_lines, names)
	else:
		print('please choose either query1, query2, query3 or query4')
