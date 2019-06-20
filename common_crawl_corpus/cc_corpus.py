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
from warcio.archiveiterator import ArchiveIterator
from alphabet_detector import AlphabetDetector
from gensim.parsing import preprocessing

class CC_Corpus(object):

	def __init__(self):
	
		#This list defines what countries to include in the corpus
		self.country_codes = [
			"ad", "ae", "af", "al", "ao", "aq", "ar", "as", "at", "au",
			"aw", "ax", "az", "ba", "bb", "bd", "be", "bf", "bg", "bh", "bi", "bj",
			"bl", "bm", "bn", "bo", "bq", "br", "bs", "bt", "bw", "by", "bz", "ca",
			"cd", "cf", "cg", "ch", "ci", "ck", "cl", "cm", "cn", "co", "cr",
			"cu", "cv", "cw", "cx", "cy", "cz", "de", "dj", "dk", "dm", "do", "dz",
			"ec", "ee", "eg", "er", "es", "et", "fi", "fj", "fk", "fm", "fo", "fr",
			"ga", "gb", "gd", "ge", "gf", "gh", "gi", "gl", "gm", "gn", "gp",
			"gr", "gt", "gu", "gw", "gy", "hk", "hn", "hr", "ht",
			"hu", "id", "ie", "il", "im", "in", "iq", "ir", "is", "it", "je", "jm",
			"jo", "jp", "ke", "kg", "kh", "ki", "km", "kn", "kp", "kr", "kw", "ky",
			"kz", "lb", "lc", "li", "lk", "lr", "ls", "lt", "lu", "lv",
			"ma", "mc", "md", "mf", "mg", "mh", "mk", "ml", "mm", "mn", "mo",
			"mp", "mq", "mr", "mt", "mu", "mv", "mw", "mx", "my", "mz", "na",
			"nc", "ne", "nf", "ng", "ni", "nl", "no", "np", "nr", "nz", "om",
			"pa", "pe", "pf", "pg", "ph", "pk", "pl", "pm", "pr", "ps", "pt",
			"pw", "py", "qa", "re", "ro", "rs", "ru", "rw", "sa", "sb", "sc", "sd",
			"se", "sg", "si", "sk", "sl", "sm", "sn", "so", "sr", "ss", "st",
			"sv", "sx", "sy", "sz", "tc", "td", "tg", "th", "tj", "tl",
			"tm", "tn", "tp", "tr", "tt", "tw", "tz", "ua", "ug", "uk",
			"us", "uy", "uz", "va", "vc", "ve", "vg", "vi", "vn", "vu", "wf",
			"ye", "yt", "za", "zm", "zw", "ελ", "бг", "бел", "мкд", "рф", "срб", "укр",
			"қаз", "հայ", "الاردن", "الجزائر", "السعودية", "المغرب", "امارات", "ایران", "بھارت", "تونس", "سودان", "سورية",
			"عراق", "عمان", "فلسطين", "قطر", "مصر", "مليسيا", "موريتانيا", "پاكستان", "پاکستان", "ڀارت", "भारत", "বাংলা",
			"ভারত", "ਭਾਰਤ", "ભારત", "இந்தியா", "இலங்கை", "சிங்கப்பூர்", "భారత్", "ಭಾರತ", "ഭാരതം", "ලංකා", "ไทย", "中国",
			"中國", "台湾", "台灣", "新加坡", "澳門", "香港", "한국"
			]
		
		#This dictionary maps country codes to (English) country names
		self.country_names = {
			"ad":"Andorra", "ae":"United_Arab_Emirates", "af":"Afghanistan", "ag":"Antigua_and_Barbuda", "al":"Albania",
			"am":"Armenia","ao":"Angola", "aq":"Antarctica", "ar":"Argentina", "as":"American_Samoa",
			"at":"Austria","au":"Australia", "aw":"Aruba", "ax":"Åland", "az":"Azerbaijan",
			"ba":"Bosnia and Herzegovina","bb":"Barbados", "bd":"Bangladesh", "be":"Belgium", "bf":"Burkina_Faso",
			"bg":"Bulgaria","bh":"Bahrain", "bi":"Burundi", "bj":"Benin", "bl":"Saint_Barthélemy",
			"bm":"Bermuda","bn":"Brunei ", "bo":"Bolivia", "bq":"Caribbean_Netherlands", "br":"Brazil",
			"bs":"Bahamas","bt":"Bhutan", "bw":"Botswana", "by":"Belarus", "bz":"Belize",
			"ca":"Canada","cc":"Cocos", "cd":"Democratic_Republic_Congo", "cf":"Central_African_Republic", "cg":"Republic_of_Congo",
			"ch":"Switzerland","ci":"Côte_d'Ivoire", "ck":"Cook_Islands", "cl":"Chile", "cm":"Cameroon",
			"cn":"China","co":"Colombia", "cr":"Costa_Rica", "cu":"Cuba", "cv":"Cabo_Verde",
			"cw":"Curaçao","cx":"Christmas_Island", "cy":"Cyprus", "cz":"Czechia", "de":"Germany",
			"dj":"Djibouti","dk":"Denmark", "dm":"Dominica", "do":"Dominican_Republic", "dz":"Algeria",
			"ec":"Ecuador","ee":"Estonia", "eg":"Egypt", "er":"Eritrea", "es":"Spain",
			"et":"Ethiopia","fi":"Finland", "fj":"Fiji", "fk":"Falkland_Islands", "fm":"Federated_States_Micronesia",
			"fo":"Faroe_Islands","fr":"France", "ga":"Gabon", "gb":"United_Kingdom ", "gd":"Grenada",
			"ge":"Georgia","gf":"French_Guiana", "gg":"Guernsey", "gh":"Ghana", "gi":"Gibraltar",
			"gl":"Greenland","gm":"Gambia", "gn":"Guinea", "gp":"Guadeloupe", "gq":"Equatorial_Guinea",
			"gr":"Greece","gs":"South_Georgia", "gt":"Guatemala", "gu":"Guam", "gw":"Guinea-Bissau",
			"gy":"Guyana","hk":"Hong_Kong", "hm":"Heard_Island", "hn":"Honduras", "hr":"Croatia",
			"ht":"Haiti","hu":"Hungary", "id":"Indonesia", "ie":"Ireland", "il":"Israel",
			"im":"Isle_of_Man","in":"India", "iq":"Iraq", "ir":"Iran", "is":"Iceland",
			"it":"Italy","je":"Jersey", "jm":"Jamaica", "jo":"Jordan", "jp":"Japan",
			"ke":"Kenya","kg":"Kyrgyzstan", "kh":"Cambodia", "ki":"Kiribati", "km":"Comoros",
			"kn":"Saint_Kitts_Nevis","kp":"North_Korea", "kr":"South_Korea", "kw":"Kuwait", "ky":"Cayman_Islands",
			"kz":"Kazakhstan","la":"Lao", "lb":"Lebanon", "lc":"Saint_Lucia", "li":"Liechtenstein",
			"lk":"Sri_Lanka","lr":"Liberia", "ls":"Lesotho", "lt":"Lithuania", "lu":"Luxembourg",
			"lv":"Latvia","ly":"Libya", "ma":"Morocco", "mc":"Monaco", "md":"Moldova",
			"me":"Montenegro","mf":"Saint-Martin", "mg":"Madagascar", "mh":"Marshall_Islands ", "mk":"North_Macedonia",
			"ml":"Mali","mm":"Myanmar", "mn":"Mongolia", "mo":"Macao", "mp":"Northern_Mariana_Islands",
			"mq":"Martinique","mr":"Mauritania", "ms":"Montserrat", "mt":"Malta", "mu":"Mauritius",
			"mv":"Maldives","mw":"Malawi", "mx":"Mexico", "my":"Malaysia", "mz":"Mozambique",
			"na":"Namibia","nc":"New_Caledonia", "ne":"Niger", "nf":"Norfolk_Island", "ng":"Nigeria",
			"ni":"Nicaragua","nl":"Netherlands", "no":"Norway", "np":"Nepal", "nr":"Nauru",
			"nu":"Niue","nz":"New_Zealand", "om":"Oman", "pa":"Panama", "pe":"Peru",
			"pf":"French_Polynesia","pg":"Papua_New_Guinea", "ph":"Philippines", "pk":"Pakistan", "pl":"Poland",
			"pm":"Saint_Pierre","pn":"Pitcairn", "pr":"Puerto Rico", "ps":"Palestine", "pt":"Portugal",
			"pw":"Palau","py":"Paraguay", "qa":"Qatar", "re":"Réunion", "ro":"Romania",
			"rs":"Serbia","ru":"Russia", "rw":"Rwanda", "sa":"Saudi_Arabia", "sb":"Solomon_Islands",
			"sc":"Seychelles","sd":"Sudan", "se":"Sweden", "sg":"Singapore", "sh":"Saint_Helena",
			"si":"Slovenia","sk":"Slovakia", "sl":"Sierra_Leone", "sm":"San_Marino", "sn":"Senegal",
			"so":"Somalia","sr":"Suriname", "ss":"South_Sudan", "st":"Sao_Tome", "sv":"El_Salvador",
			"sx":"Sint_Maarten","sy":"Syria", "sz":"Eswatini", "tc":"Caicos_Islands", "td":"Chad",
			"tf":"French_Southern","tg":"Togo", "th":"Thailand", "tj":"Tajikistan", "tk":"Tokelau",
			"tl":"Timor-Leste","tm":"Turkmenistan", "tn":"Tunisia", "to":"Tonga", "tp":"East_Timor",
			"tr":"Turkey","tt":"Trinidad_Tobago", "tv":"Tuvalu", "tw":"China", "tz":"Tanzania",
			"ua":"Ukraine","ug":"Uganda", "uk":"United_Kingdom", "us":"United_States", "uy":"Uruguay",
			"uz":"Uzbekistan","va":"The_Vatican", "vc":"Saint_Vincent", "ve":"Venezuela", "vg":"Virgin_Islands",
			"vi":"Virgin_Islands","vn":"Viet_Nam", "vu":"Vanuatu", "wf":"Wallis_Futuna", "ws":"Samoa",
			"ye":"Yemen","yt":"Mayotte", "za":"South_Africa", "zm":"Zambia", "zw":"Zimbabwe",
			"ελ":"Greece","бг":"Bulgaria", "бел":"Bulgaria", "мкд":"North_Macedonia", "рф":"Russia",
			"срб":"Serbia","укр":"Ukraine", "қаз":"Kazakhstan", "հայ":"Armenia", "الاردن":"Jordan",
			"الجزائر":"Algeria","السعودية":"Saudi_Arabia", "المغرب":"Morocco", "امارات":"United_Arab_Emirates", "ایران":"Iran",
			"بھارت":"India","تونس":"Tunisia", "سودان":"Sudan", "سورية":"Syria", "عراق":"Iraq",
			"عمان":"Oman","فلسطين":"Palestine", "قطر":"Qatar", "مصر":"Egypt", "مليسيا":"Malaysia",
			"موريتانيا":"Mauritania","پاكستان":"Pakistan", "پاکستان":"Pakistan", "ڀارت":"India", "भारत":"India",
			"বাংলা":"Bangladesh","ভারত":"India", "ਭਾਰਤ":"India", "ભારત":"India", "இந்தியா":"India",
			"இலங்கை":"Sri_Lanka","சிங்கப்பூர்":"Singapore", "భారత్":"India", "ಭಾರತ":"India", "ഭാരതം":"India",
			"ලංකා":"Thailand","ไทย":"Thailand", "中国":"China", "中國":"China", "台湾":"Taiwan",
			"台灣":"Taiwan","新加坡":"Singapore", "澳門":"Macao", "香港":"Hong_Kong", "한국":"South_Korea"                           
			}
	
		#This dictionary maps country names to large regions for organizational purposes
		self.country_regions = {
			"ad":"europe_west", "ae":"middle_east", "af":"asia_central", "al":"europe_west", "ao":"africa_sub", "aq":"antarctica", "ar":"america_south", "as":"asia_southeast",
			"at":"europe_west", "au":"oceania", "aw":"america_central", "ax":"europe_west", "az":"asia_central", "ba":"europe_east", "bb":"america_central", "bd":"asia_south",
			"be":"europe_west", "bf":"africa_sub", "bg":"europe_east", "bh":"middle_east", "bi":"africa_sub", "bj":"africa_sub", "bl":"american_central", "bm":"america_central",
			"bn":"asia_southeast", "bo":"america_south", "bq":"america_central", "br":"america_brazil", "bs":"america_central", "bt":"asia_south", "bv":"europe_west", "bw":"africa_southern",
			"by":"europe_east", "bz":"america_central", "ca":"america_north", "cd":"africa_sub", "cf":"africa_sub", "cg":"africa_sub", "ch":"europe_west", "ci":"africa_sub",
			"ck":"asia_southeast", "cl":"america_south", "cm":"africa_sub", "cn":"asia_east", "co":"america_south", "cr":"america_central", "cu":"america_central", "cv":"africa_sub",
			"cw":"america_central", "cx":"asia_southeast", "cy":"europe_west", "cz":"europe_east", "de":"europe_west", "dj":"africa_north", "dk":"europe_west", "dm":"america_central",
			"do":"america_central", "dz":"africa_north", "ec":"america_south", "ee":"europe_east", "eg":"middle_east", "er":"africa_north", "es":"europe_west", "et":"africa_north",
			"fi":"europe_west", "fj":"asia_southeast", "fk":"america_south", "fm":"asia_southeast", "fo":"europe_west", "fr":"europe_west", "ga":"africa_sub", "gb":"europe_west",
			"gd":"america_central", "ge":"asia_central", "gf":"america_south", "gh":"africa_sub", "gi":"africa_north", "gl":"europe_west", "gm":"africa_sub", "gn":"africa_sub",
			"gp":"america_central", "gr":"europe_west", "gt":"america_central", "gu":"oceania", "gw":"africa_sub", "gy":"america_south", "hk":"asia_east", "hn":"america_central",
			"hr":"europe_east", "ht":"america_central", "hu":"europe_east", "id":"asia_southeast", "ie":"europe_west", "il":"middle_east", "im":"europe_west", "in":"asia_south",
			"iq":"middle_east", "ir":"asia_central", "is":"europe_west", "it":"europe_west", "je":"europe_west", "jm":"america_central", "jo":"middle_east", "jp":"asia_east",
			"ke":"africa_sub", "kg":"asia_central", "kh":"asia_southeast", "ki":"asia_southeast", "km":"africa_sub", "kn":"america_central", "kp":"asia_east", "kr":"asia_east",
			"kw":"middle_east", "ky":"america_central", "kz":"asia_central", "lb":"middle_east", "lc":"america_central", "li":"europe_west", "lk":"asia_south", "lr":"africa_sub",
			"ls":"africa_southern", "lt":"europe_east", "lu":"europe_west", "lv":"europe_east", "ma":"africa_north", "mc":"europe_west", "md":"europe_east", "mf":"america_central",
			"mg":"africa_sub", "mh":"oceania", "mk":"europe_east", "ml":"africa_sub", "mm":"asia_southeast", "mn":"asia_east", "mo":"asia_east", "mp":"oceania",
			"mq":"america_central", "mr":"africa_sub", "mt":"europe_west", "mu":"asia_southeast", "mv":"europe_west", "mw":"africa_sub", "mx":"america_central", "my":"asia_southeast",
			"mz":"africa_sub", "na":"africa_southern", "nc":"oceania", "ne":"africa_sub", "nf":"oceania", "ng":"africa_sub", "ni":"america_central", "nl":"europe_west",
			"no":"europe_west", "np":"asia_south", "nr":"asia_southeast", "nz":"oceania", "om":"middle_east", "pa":"america_central", "pe":"america_south", "pf":"asia_southeast",
			"pg":"asia_southeast", "ph":"asia_southeast", "pk":"asia_south", "pl":"europe_east", "pm":"america_north", "pr":"america_central", "ps":"middle_east", "pt":"europe_west",
			"pw":"asia_southeast", "py":"america_south", "qa":"middle_east", "re":"africa_sub", "ro":"europe_east", "rs":"europe_east", "ru":"europe_russia", "rw":"africa_sub",
			"sa":"middle_east", "sb":"asia_southeast", "sc":"asia_south", "sd":"africa_north", "se":"europe_west", "sg":"asia_southeast", "si":"europe_east", "sk":"europe_east",
			"sl":"africa_sub", "sm":"asia_southeast", "sn":"africa_sub", "so":"africa_north", "sr":"america_south", "ss":"africa_sub", "su":"europe_russia", "sv":"america_central",
			"sx":"america_central", "sy":"middle_east", "sz":"africa_southern", "tc":"america_central", "td":"africa_sub", "tg":"africa_sub", "th":"asia_southeast", "tj":"asia_central",
			"tl":"asia_southeast", "tm":"asia_central", "tn":"africa_north", "tp":"asia_southeast", "tr":"middle_east", "tt":"america_central", "tw":"asia_east", "tz":"africa_sub",
			"ua":"europe_east", "ug":"africa_sub", "uk":"europe_west", "us":"america_north", "uy":"america_south", "uz":"asia_central", "va":"europe_west", "vc":"america_central",
			"ve":"america_south", "vg":"america_central", "vi":"america_central", "vn":"asia_southeast", "vu":"asia_southeast", "wf":"asia_southeast", "ye":"middle_east", "yt":"africa_sub",
			"za":"africa_southern", "zm":"africa_sub", "zw":"africa_southern", "ελ":"europe_west", "бг":"europe_east", "бел":"europe_east", "мкд":"europe_east", "рф":"europe_russia",
			"срб":"europe_east", "укр":"europe_east", "қаз":"asia_central", "հայ":"asia_central", "الاردن":"middle_east", "الجزائر":"africa_north", "السعودية":"middle_east", "المغرب":"middle_east",
			"امارات":"middle_east", "ایران":"middle_east", "بھارت":"asia_south", "تونس":"africa_north", "سودان":"africa_sub", "سورية":"middle_east", "عراق":"middle_east", "عمان":"middle_east",
			"فلسطين":"middle_east", "قطر":"middle_east", "مصر":"middle_east", "مليسيا":"asia_southeast", "موريتانيا":"africa_north", "پاكستان":"asia_south", "پاکستان":"asia_south", "ڀارت":"asia_south",
			"भारत":"asia_south", "বাংলা":"asia_south", "ভারত":"asia_south", "ਭਾਰਤ":"asia_south", "ભારત":"asia_south", "இந்தியா":"asia_south", "இலங்கை":"asia_south", "சிங்கப்பூர்":"asia_southeast",
			"భారత్":"asia_south", "ಭಾರತ":"asia_south", "ഭാരതം":"asia_south", "ලංකා":"asia_southeast", "ไทย":"asia_southeast", "中国":"asia_east", "中國":"asia_east", "台湾":"asia_east",
			"台灣":"asia_east", "新加坡":"asia_southeast", "澳門":"asia_east", "香港":"asia_east", "한국":"asia_east", "st": "africa_sub"
			}
	
	#----------------------------------------------------------------------------------------------#
	
	def strip_tags(self, line):

		line = re.sub(r"http\S+", "", line)
		line = re.sub("<[^>]*>", '', line)
		
		return line
	#----------------------------------------------------------------------------------------------#

	def process_wet(self, file, read_bucket):

		#Initialize emoji remover
		try:
			# Wide UCS-4 build
			myre = re.compile(u'['
				u'\U0001F300-\U0001F64F'
				u'\U0001F680-\U0001F6FF'
				u'\u2600-\u26FF\u2700-\u27BF]+', 
				re.UNICODE)
		except re.error:
			# Narrow UCS-2 build
			myre = re.compile(u'('
				u'\ud83c[\udf00-\udfff]|'
				u'\ud83d[\udc00-\ude4f\ude80-\udeff]|'
				u'[\u2600-\u26FF\u2700-\u27BF])+', 
				re.UNICODE)
		
		#Do stuff
		try:
			starting = time.time()
			line_list = []
			
			#For finding non-alphabet writing systems
			ad = AlphabetDetector()
				
			#List of illegal characters
			illegal_chars = ["|", "©", "«", "®", "»", "˂", "˃", "˄", "˅", "/", "\\", "{", "}"]
			
			client = boto3.client("s3")			
			response = client.get_object(Bucket = read_bucket, Key = file)
			
			with gzip.open(response["Body"], "r") as stream:
				for record in ArchiveIterator(stream):
					if record.rec_type == "conversion":
					
						current_url = record.rec_headers.get_header("WARC-Target-URI")
						domains = tldextract.extract(current_url)
						code = domains.suffix
						
						
						if code in self.country_codes:
							page = record.content_stream().read().decode("utf-8")
							
							current_country = self.country_names[code]
							current_region = self.country_regions[code]
							line_number = 0
							
							for line in page.splitlines():
								
								#First cut, has to be 15 characters
								if len(line) > 15:
								
									#Remove links, hashtags, at-mentions, mark-up, and "RT"
									line = re.sub(r"http\S+", "", line)
									line = re.sub(r"@\S+", "", line)
									line = re.sub(r"#\S+", "", line)
									line = re.sub("<[^>]*>", "", line)
														
									#Remove emojis
									line = re.sub(myre, "", line)
															
									#Remove extra spaces
									line = ct.pipe(line, 
													preprocessing.strip_tags, 
													preprocessing.split_alphanum,
													preprocessing.strip_multiple_whitespaces
													)
									
									#Check if still above 15
									if len(line) > 15:
									
										#Check if contains any navigational / boilerplate characters
										if not any(char in line for char in illegal_chars):
										
											#Check if all numbers / characters
											if len(ct.pipe(line, preprocessing.strip_numeric, preprocessing.strip_punctuation).replace(" ","")) > 12:
																							
												#Check if has Chinese / Japanese / Korean characters:
												try:
													if ad.is_cjk(line) or ad.is_hangul(line) or ad.is_hiragana(line) or ad.is_katakana(line):
														length = 15
												
													else:
														length = 50
												
												#Problem with character detection, default size
												except:
													length = 50
												
												#Check length threshold
												if len(line) > length:
												
													#Final check for non-text
													if line.count("-") < 4:
														if line.count("(") < 4:
															if line.count(")") < 4:
																if line.count("=") < 2:
																	if line.count("_") < 2:
																		if line.count(".") < 15:
																			if line.count("&") < 4:
																				if line.count("[") < 3:
																					if line.count("]") < 3:
																						if line.count("*") < 5:
																							line_number += 1
																							line_list.append((code, current_country, current_region, current_url, line_number, line))
						
			del response
			
			print("Loading " + str(file) + ": " + str(time.time() - starting))

			return line_list
			
		except Exception as e:
			print(e)
			print("process_wet aborted")
			
			return []
	#------------------------------------------------------------------------------------------------#

	def crawl_cc(self, prefix_list, write_bucket, workers = 1):
	
		#AWS Presets -----------------------------------#
		client = boto3.client("s3")
		read_bucket = "commoncrawl"
		
		if not isinstance(prefix_list, (list,)):
			prefix_list = [prefix_list]
			
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
			random.shuffle(segment_list)
			for segment in segment_list:
				
				#Get the write chunk
				prefix_check = segment.split("/")
				prefix_check = prefix_check[1]

				#Check if this segment has already been processed
				filename1 = segment.replace("/", ".") + "wet.hdf"
				filename2 = segment.replace("/", ".") + "wet.p"
				print("\n\t" + filename1)
				print("\t" + filename2)

				#Check S3 bucket
				check_list = []
				response1 = client.list_objects_v2(Bucket = write_bucket, Prefix = prefix_check)

				try:
					for item in response1["Contents"]:
						check_list.append(item["Key"])
				except:
					check_list = []

				if current_folder + "/" + filename1 in check_list:
					print("Already exists on s3 bucket: " + str(filename1))
					
				elif current_folder + "/" + filename2 in check_list:
					print("Already exists on s3 bucket: " + str(filename2))
					
				else:
				
					print("\t" + current_folder + "/" + filename1 + " does not exist yet.")
					print("\t" + current_folder + "/" + filename2 + " does not exist yet.")

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

					if True:

						for item in response["Contents"]:
							file_list.append(item["Key"])
						
						print(" with " + str(len(file_list)) + " files")
					
						#Loop over WET files in this segment
						#Multi-process this part
						
						if True:

							line_list = []
							
							pool_instance = mp.Pool(processes = workers, maxtasksperchild = 1)
							line_list = pool_instance.map(partial(self.process_wet,
													read_bucket = read_bucket
													), file_list, chunksize = 1)

							pool_instance.close()
							pool_instance.join()
							
							print("Done " + str(len(line_list)))

							#Done getting lines, now dedupe
							line_list = [item for item in line_list if item is not None]
							line_list = [item for sublist in line_list for item in sublist]
							current_df = pd.DataFrame(line_list)
							line_list = []
							
							if len(current_df) > 100:
							
								current_df.columns = ["Domain", "Country", "Region", "URL", "LineID", "Text"]
								starting_length = len(current_df)
								
								current_df.drop_duplicates(subset = "Text", keep = False, inplace = True)
								print("Formatted and Removed " + str(starting_length - len(current_df)) + " with total: " + str(len(current_df)))
								
								filename = segment.replace("/", ".") + "p"
									
								current_df.infer_objects()
								current_df.to_pickle(filename, compression = "gzip", protocol = 4)
								print("\tWrote " + filename)
								
								#Write to S3
								with open(filename, "rb") as data:
									filename2 = current_folder + "/" + filename
									client.upload_fileobj(data, write_bucket, filename2)
									print("\tUploaded " + filename2)
										
								#Remove from local instance
								os.remove(filename)
								del current_df
														
#----------------------------------------------------------------------------------------------------------------------#

	def format_cc(self, nickname, path_to_input, path_to_output, n_segments = 10):
	
		#Load crawl files from S3 bucket, merge and dedupe, and save to local drive
		#This process should be run on a large machine (much memory) but doesn't take long
		#Use AWS-CLI to upload files in path_to_output to S3 if desired
	
		#---- Iterate over files
		#AWS Presets -----------------------------------#
		client = boto3.client("s3")
		read_bucket = path_to_input

		response = client.list_objects_v2(
				Delimiter = "/",
				Bucket = read_bucket,
				Prefix = nickname + "/"
				)

		segment_list = []

		for item in response["Contents"]:
			segment_list.append(item["Key"])
			
		segment_list = ct.partition_all(n_segments, segment_list)
		
		full_first = True
		round_counter = 1
		round_list = []
		
		for subset in segment_list:
		
			df_list = []
			
			for filename in subset:
				
				#Open hdf
				if filename.endswith(".wet.hdf"):
					temp_name = "temp.hdf"
				elif filename.endswith(".wet.p"):
					temp_name = "temp.p"
				
				if filename.endswith(".hdf") or filename.endswith(".p"):
				
					print(filename)
					s3 = boto3.client("s3")
					
					with open(temp_name, "wb") as data:
						s3.download_fileobj(path_to_input, filename, data)
					
					if temp_name.endswith(".hdf"):
						current_df = pd.read_hdf(temp_name, key = "data")
					elif temp_name.endswith(".p"):
						current_df = pd.read_pickle(temp_name, compression = "gzip")
					
					os.remove(temp_name)
					
					#Remove unneeded countries
					country_done_list = ["Canada", 
											"Russia", 
											"Germany", 
											"France", 
											"Italy",
											"Japan",
											"Sweden",
											"Spain",
											"Netherlands",
											"Switzerland",
											"Czechia",
											"Poland",
											"Austria",
											"Belgium",
											"Denmark",
											"Finland",
											"Ireland",
											"Greece",
											"Portugal",
											"Romania",
											"Ukraine",
											"Hungary"
											]
					
					print("\t\tBefore removing countries over threshold: " + str(len(current_df)))
					current_df = current_df[~current_df["Country"].isin(country_done_list)]
					print("\t\tBefore after countries over threshold: " + str(len(current_df)))
					
					df_list.append(current_df)
					del current_df
						
			#Done with subset
			print("\tJoining subset files: " + str(len(df_list)))
			new_df = pd.concat(df_list)
			del df_list
			
			print("\tDone joining. Now deduping.")
			if True:
				
				print(len(new_df), end = "\t")
				new_df.drop_duplicates(subset = "Text", keep = "first", inplace = True)
				print(len(new_df))
			
				#Clean
				name = "round." + str(round_counter) + ".p"
				round_counter += 1
				round_list.append(name)
				new_df.to_pickle(name, compression = None, protocol = 4)
				del new_df
			
		#Done with all files
		print("Loading partial files")
		for file in round_list:
			print(file)
			
			if full_first == True:
				full_first = False
				full_df = pd.read_pickle(file)
				print("\tLength: " + str(len(full_df)))
				
			else:
				temp_df = pd.read_pickle(file)
				full_df = pd.concat([full_df, temp_df])
				del temp_df
				print("\tLength: " + str(len(full_df)))
			
		print("Final dedupe")
		starting = time.time()
		full_length = len(full_df)
		full_df.drop_duplicates(subset = "Text", keep = "first", inplace = True)
		print("Done deduplicating")
		full_df.sort_values(by = ["URL", "LineID"], inplace = True)
		
		print(str(full_length - len(full_df)) + " in " + str(time.time() - starting))
		print("Now: " + str(len(full_df)))

		#Now save country-specific files
		reverse = {v: k for k, v in self.country_names.items()}
		for country in reverse:
		
			country_code = reverse[country]	

			if country_code in self.country_regions:
				region = self.country_regions[country_code]
				
				query_string = '(Country == "' + str(country) + '")'
				current_df = full_df.query(query_string)
				current_df.infer_objects()
					
				if len(current_df) > 1000:
					
					current_count = 0	#For counting millions of samples
						
					while True:
						
						if len(current_df) > 1000000:
							write_df = current_df.head(n = 1000000)
							current_df = current_df.tail(n = len(current_df) - 1000000)
							
						else:
							write_df = current_df
							current_df = [1, 2, 3, 4, 5]
							
						current_count += 1
						name = os.path.join(region, country_code, region + "." + country_code + "." + nickname + "." + str(current_count) + ".hdf")
						write_df.to_hdf("temp.hdf", mode = "w", key = "data", format = "fixed", complevel = 9)

						s3 = boto3.resource("s3")
						s3.meta.client.upload_file("temp.hdf", path_to_output, name)
						os.remove("temp.hdf")
						
						print(country + ": " + str(len(write_df)))
							
						if len(current_df) < 1000:
							break	
			
		print("Ending!")
		del current_df
		del full_df
		
		return
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
		items = filename.split(".")

		region = items[0]
		country = items[1]
		period = items[2]
		
		return region, country, period
	#------------------------------------------------

	def get_lid_df(self, lid, current_df):

		try:
			current_text = list(current_df.loc[:,"Text"].values)
			y = lid.predict(current_text)
			
			current_df.loc[:,"Lang"] = y
			
			return current_df
			
		except:
			return None
	#------------------------------------------------
	
	def lid_cc(self, input_dir, output_dir, lid_model):
	
		#Run lidNet on the corpus
		
		#Constants
		from pathlib import Path
		from lidNet.lidNet.lidNet import lidNet
		lid = lidNet(lid_model)	

		segment_list = []
		for root, dirs, files in os.walk(input_dir):
			segment_list += files

		#Iterate over country files in current time period
		for file in segment_list:
			
			region, country, period = self.get_metadata(file)
			file = os.path.join(".", region, country, file)
			print("\tStarting " + str(file), end = "")		
			
			if file.endswith(".hdf"):
				write_name = "temp.hdf"
			elif file.endswith(".p"):
				write_name = "temp.p"

			#Download, open, and delete temp file
			current_df = self.load_df(file)
			os.remove(file)
						
			print(" with " + str(len(current_df)) + " samples")
			current_df = self.get_lid_df(lid, current_df)
			
			try:
				#Get langs present, preset the S3 path
				langs = list(set(list(current_df.loc[:,"Lang"].values)))
								
				for lang in langs:
							
					#Reduce to lang-specific df
					query_string = "(Lang == '" + lang + "')"
					lang_df = current_df.query(query_string)
								
					#Write to S3
					if not os.path.isdir(os.path.join(".", output_dir, region, country)):
						path = Path(os.path.join(".", output_dir, region, country))
						path.mkdir(parents=True)
					write_filename = os.path.join(".", output_dir, region, country, region + "." + country + "." + period + "." + lang + ".hdf")
								
					#Write to disk
					lang_df.to_hdf(write_filename, key = "data", mode = "w", format = "fixed", complevel = 9, complib = "zlib")
					
			except Exception as e:
				print("\n\n")
				print("Skipping " + file)
				print(e)
				print("\n\n")
					
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
	
	def final_cc(self, path_to_input):
	
		from collections import defaultdict
		
		#Load crawl files from local drive, merge and dedupe, and save to local drive
		#This process should be run on a large machine but doesn't take long
		#Use AWS-CLI to upload files in path_to_output to S3 if desired
	
		#---- Iterate over files
		print("")
		
		segment_list = []
		merge_dict = defaultdict(dict)
		
		for root, dirs, files in os.walk(path_to_input):
			segment_list += files

		for file in segment_list:
			print("Starting " + file)
			
			meta = file.split(".")
			region = meta[0]
			country = meta[1]
			period = meta[2]
			language = meta[3]
			
			try:
				merge_dict[country][language].append(file)
			except:
				merge_dict[country][language] = [file]
			
		for country in merge_dict:
			for language in merge_dict[country]:
				
				
				first_flag = True	#Whether to initialize holder file
				
				for file in merge_dict[country][language]:
					
					print(file)
					filename = os.path.join(path_to_input, region, country, file)

					#Open hdf
					if filename.endswith(".hdf"): 
						
						print("\t\t" + filename)
						
						try:
							current_df = pd.read_hdf(filename)
								
							if first_flag == True:
								full_df = current_df
								first_flag = False
									
							else:
								full_df = pd.concat([full_df, current_df])
							
							# #Or, open pickles
							if filename.endswith(".p"): 
							
								print("\t\t" + filename)
								current_df = pd.read_pickle(filename, compression = "gzip")
								
								if first_flag == True:
									full_df = current_df
									first_flag = False
									
								else:
									full_df = pd.concat([full_df, current_df])
						
						except Exception as e:
							print(e)
					
				#Dedupe once all country/language files have been added
				starting = time.time()
				try:
					full_length = len(full_df)
					full_df.drop_duplicates(subset = "Text", keep = "first", inplace = True)

					print("Total: " + str(len(full_df)) + ", after removing " + str((full_length - len(full_df))) + " in " + str(time.time() - starting))
					full_text = self.get_text(full_df)
					del full_df
					
					if len(full_text) > 500:
						write_dir = os.path.join(".", region, country)
						write_filename = "cc-gdc." + region + "." + country + "." + language + ".txt"
						self.write_text(full_text, write_dir, write_filename)
				
				except Exception as e:
					print(e)

		# #Now zip entire directory
		# self.zip_dir(region)