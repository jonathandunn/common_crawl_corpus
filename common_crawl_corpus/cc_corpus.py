import gzip
import tldextract
import codecs
import time
import re
import cytoolz as ct
import pandas as pd
import numpy as np
import boto3
import botocore
import os
import pathlib
import zipfile
import random
import multiprocessing as mp
from functools import partial

class CC_Corpus(object):

	def __init__(self):
	
		#This list defines what countries to include in the corpus
		self.country_codes = [
			"td", "kp", "et", "cy", "mq", "ki", "cg", "nr", "mv", "bj",
			"gh", "gf", "zm", "eg", "ne", "pa", "ls", "sz", "gl", "je",
			"gp", "ad", "pm", "mt", "tr", "kn", "wf", "fo", "ck", "ax",
			"gy", "cw", "mw", "mo", "bs", "dm", "mp", "om", "bb", "tg",
			"mc", "sd", "sv", "mz", "nf", "vi", "na", "lc", "iq", "py",
			"bf", "km", "bt", "mr", "yt", "aw", "sr", "rw", "sl", "im",
			"ao", "bi", "ci", "nc", "cm", "bh", "bo", "ai", "af", "gm",
			"gi", "pf", "sx", "vu", "uy", "sm", "mk", "cv", "dj", "sa",
			"mg", "as", "ug", "nz", "gd", "cd", "pr", "bw", "ht", "tn",
			"al", "ng", "ve", "gt", "tj", "vc", "lu", "dz", "jo", "qa",
			"vg", "id", "so", "by", "au", "sn", "tt", "sy", "md", "sc",
			"ps", "mu", "tc", "tm", "bm", "su", "ee", "lv", "tw", "cr",
			"tl", "ua", "ar", "bz", "ba", "is", "kg", "si", "lt", "pw",
			"hk", "bg", "fi", "jp", "ge", "kr", "ph", "pk", "az", "rs",
			"mn", "pt", "my", "ec", "cn", "sk", "at", "ae", "hr", "pe",
			"sg", "lk", "za" 
			]
		
		#This dictionary maps country codes to (English) country names
		self.country_names = {
			"ad":"Andorra", "ae":"United Arab Emirates", "af":"Afghanistan", "ai":"Anguilla", "al":"Albania", "an":"Netherlands Antilles",
			"ao":"Angola", "ar":"Argentina", "as":"American Samoa", "at":"Austria", "au":"Australia", "aw":"Aruba", "ax":"Axland", "az":"Azerbaijan",
			"ba":"Bosnia and Herzegovina", "bb":"Barbados", "bd":"Bangladesh", "be":"Belgium", "bf":"Burkina Faso", "bg":"Bulgaria", "bh":"Bahrain",
			"bi":"Burundi", "bj":"Benin", "bm":"Bermuda", "bn":"Brunei", "bo":"Bolivia", "bq":"Bonaire", "br":"Brazil", "bs":"Bahamas", "bt":"Bhutan",
			"bv":"Bouvet Island", "bw":"Botswana", "by":"Belarus", "bz":"Belize", "ca":"Canada", "cd":"Democratic Republic of the Congo", "cg":"Republic of the Congo",
			"ch":"Switzerland", "ci":"Coast d'Ivoire", "ck":"Cook Islands", "cl":"Chile", "cm":"Cameroon", "cn":"People's Republic of China", "cr":"Costa Rica",
			"cu":"Cuba", "cv":"Cape Verde", "cw":"Curacao", "cx":"Christmas Island", "cy":"Cyprus", "cz":"Czech Republic", "de":"Germany", "dj":"Djibouti",
			"dk":"Denmark", "dm":"Dominica", "do":"Dominican Republic", "dz":"Algeria", "ec":"Ecuador", "ee":"Estonia", "eg":"Egypt", "eh":"Western Sahara",
			"er":"Eritrea", "es":"Spain", "et":"Ethiopia", "eu":"European Union", "fi":"Finland", "fj":"Fiji", "fk":"Falkland Islands", "fm":"Federated States of Micronesia",
			"fo":"Faroe Islands", "fr":"France", "gb":"United Kingdom", "gd":"Grenada", "ge":"Georgia", "gf":"French Guiana", "gh":"Ghana", "gi":"Gibraltar",
			"gl":"Greenland", "gm":"The Gambia", "gn":"Guinea", "gp":"Guadeloupe", "gr":"Greece", "gt":"Guatemala", "gu":"Guam", "gw":"Guinea-Bissau", "gy":"Guyana",
			"hk":"Hong Kong", "hn":"Honduras", "hr":"Croatia", "ht":"Haiti", "hu":"Hungary", "id":"Indonesia", "ie":"Ireland", "il":"Israel", "im":"Isle of Man",
			"in":"India", "iq":"Iraq", "ir":"Iran", "is":"Iceland", "it":"Italy", "je":"Jersey", "jm":"Jamaica", "jo":"Jordan", "jp":"Japan", "ke":"Kenya", "kg":"Kyrgyzstan",
			"kh":"Cambodia", "ki":"Kiribati", "km":"Comoros", "kn":"Saint Kitts and Nevis", "kp":"North Korea", "kr":"South Korea", "kw":"Kuwait", "kz":"Kazakhstan",
			"lb":"Lebanon", "lc":"Saint Lucia", "lk":"Sri Lanka", "lr":"Liberia", "ls":"Lesotho", "lt":"Lithuania", "lu":"Luxembourg", "lv":"Latvia", "mc":"Monaco",
			"md":"Moldova", "mf":"Collectivity of Saint Martin", "mg":"Madagascar", "mh":"Marshall Islands", "mk":"Macedonia", "mm":"Myanmar", "mn":"Mongolia",
			"mo":"Macau", "mp":"Northern Mariana Islands", "mq":"Martinique", "mr":"Mauritania", "mt":"Malta", "mu":"Mauritius", "mv":"Maldives", "mw":"Malawi",
			"mx":"Mexico", "my":"Malaysia", "mz":"Mozambique", "na":"Namibia", "nc":"New Caledonia", "ne":"Niger", "nf":"Norfolk Island", "ng":"Nigeria", "ni":"Nicaragua",
			"nl":"Netherlands", "no":"Norway", "np":"Nepal", "nr":"Nauru", "nz":"New Zealand", "om":"Oman", "pa":"Panama", "pe":"Peru", "pf":"French Polynesia", "pg":"Papua New Guinea",
			"ph":"Philippines", "pk":"Pakistan", "pl":"Poland", "pm":"Saint-Pierre and Miquelon", "pr":"Puerto Rico", "ps":"Palestine", "pt":"Portugal", "pw":"Palau",
			"py":"Paraguay", "qa":"Qatar", "re":"Reunion", "ro":"Romania", "rs":"Serbia", "ru":"Russia", "rw":"Rwanda", "sa":"Saudi Arabia", "sb":"Solomon Islands", "sc":"Seychelles",
			"sd":"Sudan", "se":"Sweden", "sg":"Singapore", "si":"Slovenia", "sk":"Slovakia", "sl":"Sierra Leone", "sm":"San Marino", "sn":"Senegal", "so":"Somalia", "sr":"Suriname",
			"ss":"South Sudan", "su":"Soviet Union", "sv":"El Salvador", "sx":"Sint Maarten", "sy":"Syria", "sz":"Swaziland", "tc":"Turks and Caicos Islands", "td":"Chad",
			"tg":"Togo", "th":"Thailand", "tj":"Tajikistan", "tl":"East Timor", "tm":"Turkmenistan", "tn":"Tunisia", "tp":"East Timor", "tr":"Turkey", "tt":"Trinidad and Tobago",
			"tw":"Taiwan", "tz":"Tanzania", "ua":"Ukraine", "ug":"Uganda", "uk":"United Kingdom", "us":"United States of America", "uy":"Uruguay", "uz":"Uzbekistan",
			"va":"Vatican City", "vc":"Saint Vincent and the Grenadines", "ve":"Venezuela", "vg":"British Virgin Islands", "vi":"United States Virgin Islands", "vn":"Vietnam", "vu":"Vanuatu",
			"wf":"Wallis and Futuna", "ye":"Yemen", "yt":"Mayotte", "za":"South Africa", "zm":"Zambia", "zw":"Zimbabwe"
			}
	
		#This dictionary maps country names to large regions for organizational purposes
		self.country_regions = {
			"ad":"europe_west", "ae":"middle_east", "af":"asia_central", "ai":"america_central", "al":"europe_west", "an":"america_central", "ao":"africa_sub", "ar":"america_south",
			"as":"asia_southeast", "at":"europe_west", "au":"oceania", "aw":"america_central", "ax":"europe_west", "az":"asia_central", "ba":"europe_east", "bb":"america_central",
			"bd":"asia_south", "be":"europe_west", "bf":"africa_sub", "bg":"europe_east", "bh":"middle_east", "bi":"africa_sub", "bj":"africa_sub", "bm":"america_central", "bn":"asia_southeast",
			"bo":"america_south", "bq":"america_central", "br":"america_brazil", "bs":"america_central", "bt":"asia_south", "bv":"europe_west", "bw":"africa_southern", "by":"europe_east",
			"bz":"america_central", "ca":"america_north", "cd":"africa_sub", "cg":"africa_sub", "ch":"europe_west", "ci":"africa_sub", "ck":"asia_southeast", "cl":"america_south", "cm":"africa_sub",
			"cn":"asia_east", "cr":"america_central", "cu":"america_central", "cv":"africa_sub", "cw":"america_central", "cx":"asia_southeast", "cy":"europe_west", "cz":"europe_east", "de":"europe_west",
			"dj":"africa_north", "dk":"europe_west", "dm":"america_central", "do":"america_central", "dz":"africa_north", "ec":"america_south", "ee":"europe_east", "eg":"middle_east", "eh":"africa_north",
			"er":"africa_north", "es":"europe_west", "et":"africa_north", "eu":"europe_west", "fi":"europe_west", "fj":"asia_southeast", "fk":"america_south", "fm":"asia_southeast", "fo":"europe_west",
			"fr":"europe_west", "gb":"europe_west", "gd":"america_central", "ge":"asia_central", "gf":"america_south", "gh":"africa_sub", "gi":"africa_north", "gl":"europe_west", "gm":"africa_sub",
			"gn":"africa_sub", "gp":"america_central", "gr":"europe_west", "gt":"america_central", "gu":"oceania", "gw":"africa_sub", "gy":"america_south", "hk":"asia_east", "hn":"america_central",
			"hr":"europe_east", "ht":"america_central", "hu":"europe_east", "id":"asia_southeast", "ie":"europe_west", "il":"middle_east", "im":"europe_west", "in":"asia_south", "iq":"middle_east",
			"ir":"asia_central", "is":"europe_west", "it":"europe_west", "je":"europe_west", "jm":"america_central", "jo":"middle_east", "jp":"asia_east", "ke":"africa_sub", "kg":"asia_central",
			"kh":"asia_southeast", "ki":"asia_southeast", "km":"africa_sub", "kn":"america_central", "kp":"asia_east", "kr":"asia_east", "kw":"middle_east", "kz":"asia_central", "lb":"middle_east",
			"lc":"america_central", "lk":"asia_south", "lr":"africa_sub", "ls":"africa_southern", "lt":"europe_east", "lu":"europe_west", "lv":"europe_east", "mc":"europe_west", "md":"europe_east",
			"mf":"america_central", "mg":"africa_sub", "mh":"oceania", "mk":"europe_east", "mm":"asia_southeast", "mn":"asia_east", "mo":"asia_east", "mp":"oceania", "mq":"america_central", "mr":"africa_sub",
			"mt":"europe_west", "mu":"asia_southeast", "mv":"europe_west", "mw":"africa_sub", "mx":"america_central", "my":"asia_southeast", "mz":"africa_sub", "na":"africa_southern", "nc":"oceania",
			"ne":"africa_sub", "nf":"oceania", "ng":"africa_sub", "ni":"america_central", "nl":"europe_west", "no":"europe_west", "np":"asia_south", "nr":"asia_southeast", "nz":"oceania", "om":"middle_east",
			"pa":"america_central", "pe":"america_south", "pf":"asia_southeast", "pg":"asia_southeast", "ph":"asia_southeast", "pk":"asia_south", "pl":"europe_east", "pm":"america_north", "pr":"america_central",
			"ps":"middle_east", "pt":"europe_west", "pw":"asia_southeast", "py":"america_south", "qa":"middle_east", "re":"africa_sub", "ro":"europe_east", "rs":"europe_east", "ru":"europe_russia", "rw":"africa_sub",
			"sa":"middle_east", "sb":"asia_southeast", "sc":"asia_south", "sd":"africa_north", "se":"europe_west", "sg":"asia_southeast", "si":"europe_east", "sk":"europe_east", "sl":"africa_sub", "sm":"asia_southeast",
			"sn":"africa_sub", "so":"africa_north", "sr":"america_south", "ss":"africa_sub", "su":"europe_russia", "sv":"america_central", "sx":"america_central", "sy":"middle_east", "sz":"africa_southern", "tc":"america_central",
			"td":"africa_sub", "tg":"africa_sub", "th":"asia_southeast", "tj":"asia_central", "tl":"asia_southeast", "tm":"asia_central", "tn":"africa_north", "tp":"asia_southeast", "tr":"middle_east",
			"tt":"america_central", "tw":"asia_east", "tz":"africa_sub", "ua":"europe_east", "ug":"africa_sub", "uk":"europe_west", "us":"america_north", "uy":"america_south", "uz":"asia_central", "va":"europe_west",
			"vc":"america_central", "ve":"america_south", "vg":"america_central", "vi":"america_central", "vn":"asia_southeast", "vu":"asia_southeast", "wf":"asia_southeast", "ye":"middle_east", "yt":"africa_sub",
			"za":"africa_southern", "zm":"africa_sub", "zw":"africa_southern"
			}
	
	#----------------------------------------------------------------------------------------------#
	
	def strip_tags(self, line):

		line = re.sub(r"http\S+", "", line)
		line = re.sub("<[^>]*>", '', line)
		
		return line
	#----------------------------------------------------------------------------------------------#

	def process_wet(self, file, read_bucket):

		starting = time.time()
		line_list = []
		
		client = boto3.client("s3")			
		response = client.get_object(Bucket = read_bucket, Key = file)
				
		with gzip.open(response["Body"], "r") as fo:
					
			for line in fo:
							
				line = line.decode("utf-8").strip()
							
				if line == "WARC/1.0":
					country_flag = 0
								
				#Get and format url
				elif line[0:15] == "WARC-Target-URI":
							
					line = line[17:]
					url = line
					domains = tldextract.extract(line)
					code = domains.suffix
								
					#Set flag to indicate allowed code
					if code in self.country_codes:
						country_flag = 1
									
				elif country_flag == 1:
								
					line = self.strip_tags(line)
					words = line.split(" ")
					
					#Many short texts are not real samples; limit text length
					if len(words) > 40:
								
						if line.count("-") < 5 and line.count("|") < 2 and line.count("/") < 5:
							if line.count("rror") < 2:
								line_list.append((code, url, line))
					
		print("Loading " + str(file) + ": " + str(time.time() - starting))
		
		return line_list
	#------------------------------------------------------------------------------------------------#

	def crawl_cc(self, prefix_list, write_bucket, workers = 1):
	
		#AWS Presets -----------------------------------#
		client = boto3.client("s3")
		read_bucket = "commoncrawl"
			
		for current_prefix in prefix_list:

			current_folder = current_prefix
			current_prefix = "crawl-data/" + current_prefix + "/segments/"

			response = client.list_objects_v2(
				Delimiter = "/",
				Bucket = read_bucket,
				Prefix = current_prefix
				)
				
			segment_list = []

			for item in response["CommonPrefixes"]:
				segment_list.append(item["Prefix"])

			#Loop over segments of selected crawl ------------------#
			for segment in segment_list:
				
				#Get the write chunk
				prefix_check = segment.split("/")
				prefix_check = prefix_check[1]

				#Check if this segment has already been processed
				filename = segment.replace("/", ".") + "wet.p"
				print("\n\n\t" + filename)
				
				#Check S3 bucket
				check_list = []
				response1 = client.list_objects_v2(Bucket = write_bucket, Prefix = prefix_check)

				try:
					for item in response1["Contents"]:
						check_list.append(item["Key"])
				except:
					check_list = []

				if os.path.isfile(filename):
					print("Already exists on local disk: " + str(filename))
				
				elif current_folder + "/" + filename in check_list:
					print("Already exists on s3 bucket: " + str(filename))
					
				else:
				
					print("\t" + current_folder + "/" + filename + " does not exist yet.")
					#Script initialization  --------------------------------#
					country_flag = 0
					line_list = []
					
					segment = segment + "wet/"
					print("\n\n\tStarting " + str(segment), end = "")
					
					response = client.list_objects_v2(
					Delimiter = "/",
					Bucket = read_bucket,
					Prefix = segment
					)

					file_list = []

					try:
						for item in response["Contents"]:
							file_list.append(item["Key"])
						
						print(" with " + str(len(file_list)) + " files")
					
						#Loop over WET files in this segment
						#Multi-process this part
						
						try:
							line_list = []
							
							pool_instance = mp.Pool(processes = workers, maxtasksperchild = 1)
							line_list = pool_instance.map(partial(self.process_wet,
													read_bucket = read_bucket
													), file_list, chunksize = 1)

							pool_instance.close()
							pool_instance.join()
							
							print("Done " + str(len(line_list)))

							#Done getting lines, now dedupe
							
							line_list = [item for sublist in line_list for item in sublist]
							current_df = pd.DataFrame(line_list)
							line_list = []
							
							if len(current_df) > 100:
							
								current_df.columns = ["Country", "URL", "Text"]
								starting_length = len(current_df)
								
								current_df.drop_duplicates(subset = "Text", keep = False, inplace = True)
								print("Formatted and Removed " + str(starting_length - len(current_df)) + " with total: " + str(len(current_df)))
								
								filename = segment.replace("/", ".") + "p"
								current_df.to_pickle(filename, compression = "gzip", protocol = 4)
								print("\tWrote " + filename)
								
								#Write to S3
								with open(filename, "rb") as data:
									filename2 = current_folder + "/" + filename
									client.upload_fileobj(data, write_bucket, filename2)
									print("\tUploaded " + filename2)
									
								#Remove from local instance
								os.remove(filename)
								
						except Exception as e:
							print(e)
							
					except Exception as e:
							print(e)
						
#----------------------------------------------------------------------------------------------------------------------#

	def format_cc(self, nickname, path_to_input, path_to_output):
	
		#Load crawl files from local drive, merge and dedupe, and save to local drive
		#This process should be run on a large machine but doesn't take long
		#Use AWS-CLI to upload files in path_to_output to S3 if desired
	
		#---- Iterate over files
		first_flag = True

		for filename in os.listdir(path_to_input):
			
			filename = os.path.join(path_to_input, filename)
			
			#Open hdf
			if filename.endswith(".wet.hdf"): 
			
				print(filename)
				current_df = pd.read_hdf(filename)
				
				if first_flag == True:
					full_df = current_df
					first_flag = False
					
				else:
					full_df = pd.concat([full_df, current_df])
			
			#Or, open pickles
			if filename.endswith(".wet.p"): 
			
				print(filename)
				current_df = pd.read_pickle(filename, compression = "gzip")
				
				if first_flag == True:
					full_df = current_df
					first_flag = False
					
				else:
					full_df = pd.concat([full_df, current_df])
					
		#Dedupe
		starting = time.time()
		full_length = len(full_df)
		full_df.drop_duplicates(subset = "Text", keep = False, inplace = True)

		print(str(full_length - len(full_df)) + " in " + str(time.time() - starting))
				
		#Now save the full file
		#full_df.to_hdf("Full_Dataset.hdf", key = "Table", format = "fixed", mode = "w", complevel = 9)

		#Now save country-specific files
		for country in self.country_codes:
			
			if country in self.country_regions:
				
				region = self.country_regions[country]
			
				query_string = "(Country == '" + str(country) + "')"
				current_df = full_df.query(query_string)
				
				current_df.infer_objects()
				
				if len(current_df) > 10:
				
					#Cap length of current_df
					if len(current_df) > 5000000:
						current_df = current_df.sample(n = 5000000, replace = False)
				
					try:
					
						name = os.path.join(path_to_output, region + "." + country + "." + nickname + ".p")
						current_df.to_pickle(name, compression = "gzip", protocol = 4)		
						print(country + ": " + str(len(current_df)))
						
					except:
					
						print(country + ": " + str(len(current_df)) + " ERROR!")
	#------------------------------------------------------------------------------------------------------------#
	
	def load_df(self, file):

		if file.endswith(".hdf"):
			current_df = pd.read_hdf(file)
		
		elif file.endswith(".p"):
			current_df = pd.read_pickle(file, compression = "gzip")
			
		#Cap length of current_df
		if len(current_df) > 3000000:
			current_df = current_df.sample(n = 3000000, replace = False)
		
		return current_df
		#------------------------------------------------

	def get_metadata(self, filename):

		#Break path
		items = filename.split("/")
		items = items[2].split(".")

		region = items[0]
		country = items[1]
		period = items[2]
		
		return region, country, period
	#------------------------------------------------

	def get_lid_df(self, lid, current_df):

		current_text = list(current_df.loc[:,"Text"].values)
		y = lid.predict(current_text)
		
		current_df.loc[:,"Lang"] = y
		
		return current_df
	#------------------------------------------------
	
	def lid_cc(self, read_bucket, write_bucket, prefix_list, lid_model):
	
		#Run lidNet on the corpus
		
		#Constants
		lid = lidNet(lid_model, n_gram_tuple = (1,3))	

		client = boto3.client("s3")

		for current_prefix in prefix_list:

			print("\n\nStarting " + str(current_prefix))
			
			#Get list of files in this prefix
			response = client.list_objects_v2(
				Delimiter = "/",
				Bucket = read_bucket,
				Prefix = current_prefix
				)

			segment_list = []

			for item in response["CommonPrefixes"]:
				new_prefix = item["Prefix"]
				
				#Get list of files in this prefix
				new_response = client.list_objects_v2(
					Delimiter = "/",
					Bucket = read_bucket,
					Prefix = new_prefix
					)
				
				for new_item in new_response["Contents"]:				
					segment_list.append(new_item["Key"])
				
			#Iterate over country files in current time period
			for file in segment_list:
			
				if True:
			
					print("\tStarting " + str(file), end = "")
					region, country, period = self.get_metadata(file)
					
					if file.endswith(".hdf"):
						write_name = "temp.hdf"
					elif file.endswith(".p"):
						write_name = "temp.p"

					#Download, open, and delete temp file
					client.download_file(read_bucket, file, write_name)
					current_df = self.load_df(write_name)
					os.remove(write_name)		
						
					print(" with " + str(len(current_df)) + " samples")
					current_df = self.get_lid_df(lid, current_df)
					
					#Get langs present, preset the S3 path
					langs = list(set(list(current_df.loc[:,"Lang"].values)))
							
					for lang in langs:
						
						#Reduce to lang-specific df
						query_string = "(Lang == '" + lang + "')"
						lang_df = current_df.query(query_string)
						
						#Write to S3
						current_path = region + "/" + country + "/" + lang + "/"
						write_filename = region + "." + country + "." + period + "." + lang + ".hdf"
						
						#Write to disk
						lang_df.to_hdf(write_filename, key = "data", mode = "w", format = "fixed", complevel = 9, complib = "zlib")
						
						#Upload to S3, remove local copy
						client.upload_file(write_filename, write_bucket, current_path + write_filename)
						os.remove(write_filename)
						
				#except Exception as e:
				#	print(e, file)
	
	#--------------------------------------------------------------------
	
	def get_text(self, full_df):
	
		full_text = []
		text = list(full_df.loc[:,"Text"].values)
		
		word_counter = 0
		temp_string = ""
		
		for sample in text:
			for word in sample.split(" "):
				
				word_counter += 1
				temp_string += word + " "
				
				if word_counter >= 100:
					full_text.append(temp_string[:-1])
					word_counter = 0
					temp_string = ""
				
		return full_text
	#--------------------------------------------------------------------
	
	def write_text(self, full_text, write_dir, write_filename):
	
		pathlib.Path(write_dir).mkdir(parents = True, exist_ok = True)
		
		random.shuffle(full_text)
	
		with codecs.open(os.path.join(write_dir, write_filename), "w", encoding = "utf-8", errors = "replace") as fo:
			for line in full_text:
				fo.write(str(line))
				fo.write("\n")
				
	#--------------------------------------------------------------------
	
	def zip_dir(self, region):
	
		print("\nWriting to zip file.\n")
		path = os.path.join(".", region)	
		zipf = zipfile.ZipFile(region + ".zip", "w", zipfile.ZIP_DEFLATED)
		
		for root, dirs, files in os.walk(path):
			for file in files:
				zipf.write(os.path.join(root, file))
	
	#-------------------------------------------------------------------
	
	def final_cc(self, region, path_to_input):
	
		#Load crawl files from local drive, merge and dedupe, and save to local drive
		#This process should be run on a large machine but doesn't take long
		#Use AWS-CLI to upload files in path_to_output to S3 if desired
	
		#---- Iterate over files
		print("")
		path_to_output = os.path.join(".", region)

		for country in os.listdir(path_to_input):
			
			print("Starting " + country)
			country_path = os.path.join(path_to_input, country)

			for language in os.listdir(country_path):
			
				print("\tStarting " + country + ": " + language, end = "\t")
				language_path = os.path.join(country_path, language)
				
				first_flag = True	#Whether to initialize holder file
				
				for filename in os.listdir(language_path):
					filename = os.path.join(path_to_input, country, language, filename)

					#Open hdf
					if filename.endswith(".hdf"): 
					
						#print("\t\t" + filename)
						current_df = pd.read_hdf(filename)
						
						if first_flag == True:
							full_df = current_df
							first_flag = False
							
						else:
							full_df = pd.concat([full_df, current_df])
					
					#Or, open pickles
					if filename.endswith(".p"): 
					
						#print("\t\t" + filename)
						current_df = pd.read_pickle(filename, compression = "gzip")
						
						if first_flag == True:
							full_df = current_df
							first_flag = False
							
						else:
							full_df = pd.concat([full_df, current_df])
					
				#Dedupe once all country/language files have been added
				starting = time.time()
				full_length = len(full_df)
				full_df.drop_duplicates(subset = "Text", keep = "first", inplace = True)

				print("Total: " + str(len(full_df)) + ", after removing " + str((full_length - len(full_df))) + " in " + str(time.time() - starting))
				full_text = self.get_text(full_df)
				del full_df
				
				if len(full_text) > 500:
					
					write_dir = os.path.join(path_to_output, region, country)
					write_filename = "cc-gdc." + region + "." + country + "." + language + ".txt"
					self.write_text(full_text, write_dir, write_filename)

		#Now zip entire directory
		self.zip_dir(region)