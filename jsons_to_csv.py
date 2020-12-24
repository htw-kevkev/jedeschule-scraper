from pathlib import Path
import json
import os
import csv

def string_cleaning(col):
	return col.replace(',',' ').replace(';', ' ').replace('\t',' ').replace('\n', ' ').replace('\r',' ')

# name, id, address, address2, zip, city, website, email, school_type, legal_status, provider, fax, phone, director

all_data = []
all_data.append(['name', 'id', 'address', 'address2', 'zip', 'city', 'website', 'email'
	, 'school_type', 'legal_status', 'provider', 'fax', 'phone', 'director'])

path = Path(__file__) / "../data/"
for file in os.listdir(path):
	# if file.endswith(".json") and file == "bremen.json":
	if file.endswith(".json"):
		with open(os.path.join(path, file), encoding="utf8") as json_file:
			data = json.load(json_file)
			for d in data:
				name = string_cleaning(d['info']['name'])
				id = string_cleaning(d['info']['id'])
				try:
					address = string_cleaning(d['info']['address'])
				except:
					address = None

				try:
					address2 = string_cleaning(d['info']['address2'])
				except:
					address2 = None

				try:
					zip = string_cleaning(d['info']['zip'])
				except:
					zip = None

				try:
					city = string_cleaning(d['info']['city'])
				except:
					city = None

				try:
					website = string_cleaning(d['info']['website'])
				except:
					website = None

				try:
					email = string_cleaning(d['info']['email'])
				except:
					email = None

				try:
					school_type = string_cleaning(d['info']['school_type'])
				except:
					school_type = None

				try:
					legal_status = string_cleaning(d['info']['legal_status'])
				except:
					legal_status = None

				try:
					provider = string_cleaning(d['info']['provider'])
				except:
					provider = None
				
				try:
					fax = string_cleaning(d['info']['fax'])
				except:
					fax = None

				try:
					phone = string_cleaning(d['info']['phone'])
				except:
					phone = None
				
				try:
					director = string_cleaning(d['info']['director'])
				except:
					director = None

				all_data.append([name, id, address, address2, zip, city, website, email, school_type, legal_status, provider, fax, phone, director])
	else:
		continue

path_csv = Path(__file__) / "../data/all_data.csv"
with open(path_csv, mode='w', newline='', encoding="utf8") as csvfile:
	writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
	for d in all_data:
		writer.writerow(d)