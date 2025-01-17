# This programme converts the zst file into csv file whilst filtering the file using keywords.
# Edit config.json to change search keywords or columns to copy over.
#
# Use case example:
# python3 to_csv_m.py wallstreetbets_submissions.zst wallstreetbets_submissions.csv

from datetime import datetime
import logging.handlers
import zstandard
import json
import csv
import sys
import os
import re

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")
			
			for line in lines[:-1]:
				yield line, file_handle.tell()
				
			buffer = lines[-1]
		reader.close()

def search_str(pattern, search_fields, obj):
	for col in search_fields:
		text = str(obj[col]).encode("utf-8", errors='replace').decode().lower()
		if re.search(pattern, text):
			return True
	return False

if __name__ == "__main__":
	# input_list_file_path = sys.argv[1]
	# output_list_file_path = sys.argv[2]
	
	start_date = sys.argv[1].split('-') # ex. 2018-01
	end_date = sys.argv[2].split('-') # ex. 2019-12
	
	__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
	
	with open(os.path.join(__location__, 'config.json')) as f:
		config = json.load(f)
	
	input_dir = config["input_dir"]
	output_dir = config["output_dir"]
	
	year, month = int(start_date[0]), int(start_date[1])
	end_year, end_month = int(end_date[0]), int(end_date[1])
	
	# input_file_paths = []
	# with open(input_list_file_path, 'r') as file:
	# 	for line in file:
	# 		input_file_paths.append(line.strip())
	# 
	# output_file_paths = []
	# with open(output_list_file_path, 'r') as file:
	# 	for line in file:
	# 		output_file_paths.append(line.strip())
	
	input_file_paths = []
	output_file_paths = []
	
	while year <= end_year:
		while month <= 12:
			input_file_paths.append(input_dir + 'RS_' + f'{year:02}' + '-' + f'{month:02}' + '.zst')
			output_file_paths.append(output_dir + 'RS_' + f'{year:02}' + '-' + f'{month:02}' + '.csv')
			
			if year >= end_year and month >= end_month: break
			month += 1
		year += 1
		month = 1
		
	columns = [i.strip() for i in config["columns"].split(",")]
	keyword_search = config["keyword_search"]
	search_fields = [i.strip() for i in config["search_fields"].split(",")]
	
	keywords = [i.strip().lower() for i in config["keywords"].split(",")]
	escaped_keywords = [re.escape(keyword) for keyword in keywords]
	pattern = r'(?i)\b(\$?{})\b'.format(r'\b|\b'.join(escaped_keywords))
	# print("PATTERN:", pattern)

	for i in range(len(input_file_paths)):
		input_file_path = input_file_paths[i]
		output_file_path = output_file_paths[i]
		print('\n' + input_file_path)
		
		file_size = os.stat(input_file_path).st_size
		file_lines = 0
		file_bytes_processed = 0
		line = None
		created = None
		bad_lines = 0
		added_lines = 0
		output_file = open(output_file_path, "w", encoding='utf-8', newline="")
		writer = csv.writer(output_file, escapechar='\\')
		writer.writerow(columns)
		try:
			for line, file_bytes_processed in read_lines_zst(input_file_path):
				try:
					obj = json.loads(line)
					# print(obj.keys())
					# break
					output_obj = []
					if keyword_search:
						if search_str(pattern, search_fields, obj):
							for field in columns:
								output_obj.append(str(obj[field]).encode("utf-8", errors='replace').decode())
							writer.writerow(output_obj)
							added_lines += 1
					else:
						for field in columns:
							output_obj.append(str(obj[field]).encode("utf-8", errors='replace').decode())
						writer.writerow(output_obj)
						added_lines += 1
						
					created = datetime.utcfromtimestamp(int(obj['created_utc']))
				except json.JSONDecodeError as err:
					bad_lines += 1
				file_lines += 1
				if file_lines % 100000 == 0:
					log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {added_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")
		except KeyError as err:
			log.info(f"Object has no key: {err}")
			log.info(line)
		except Exception as err:
			log.info(err)
			log.info(line)
			
		output_file.close()
		log.info(f"Complete : {file_lines:,} : {bad_lines:,}")