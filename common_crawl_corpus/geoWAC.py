#This script processes the main CGLU corpus, 
# checks the LID predictions using CLD2 and CLD3, 
# deduplicates again by country and by language,
# and saves a corpus file

import os
import random
import time
import cld3
import cld2
import psutil
import gc
import pandas as pd
import cytoolz as ct
import multiprocessing as mp
from functools import partial

#-----------------------------------------------
def get_lid(line, threshold = 150):

	#CLD2
	if len(line) > threshold:
		try:
			isReliable, textBytesFound, details = cld2.detect(line, isPlainText = True)
			code_cld2 = details[0][1]
			code_cld2 = mapping_dict[code_cld2]
		except:
			code_cld2 = "ukn"
			
	#CLD3
		try:
			prediction = cld3.get_language(line)
			code_cld3 = prediction[0]
			code_cld3 = mapping_dict[code_cld3]
		except:
			code_cld3 = "ukn"
	else:
		code_cld2 = "ukn"
		code_cld3 = "ukn"

	return code_cld2, code_cld3

#-----------------------------------------------
def process_file(file, language, workers = 64):

	start = time.time()
	
	while True:
		try:
			df = pd.read_csv(file)
			break
		except Exception as e:
			print(e)
			time.sleep(10)
			
	pages = df.loc[:,"Text"].values
	del df
	pages = [str(x).split("\n") for x in pages]
	pages = list(ct.concat(pages))

	#Multi-process
	pool_instance = mp.Pool(processes = workers, maxtasksperchild = None)
	codes = pool_instance.map(get_lid, pages, chunksize = 100)
	pool_instance.close()
	pool_instance.join()
	
	pages = [pages[i] for i in range(len(pages)) if codes[i][0] == language and codes[i][1] == language]
	print("\t" + file + "  " + str(time.time() - start) + "  with  " + str(len(pages)))
	
	return pages

#-----------------------------------------------

if __name__ == "__main__":

	temp_folder = "lrec"	#Folder to store temp files, will be cleaned at end
	country_limit = 150		#Number of files per country to allow
	langs = ["fra", "spa"]	#List of language codes to process
	
	#Dictionary with Country_Name: ISO-3 mappings	
	iso_dict = {"Algeria":"dza", "Djibouti":"dji", "Eritrea":"eri", "Eswatini":"swv", "Ethiopia":"eth", "Libya":"lby", "Morocco":"mar", "Somalia":"som",
	"Sudan":"sdn", "Tunisia":"tun", "Botswana":"bwa", "Lesotho":"lso", "South_Africa":"zaf", "Zimbabwe":"zwe", "Angola":"ago", "Benin":"ben",
	"Burkina_Faso":"bfa", "Burundi":"bdi", "Côte d'Ivoire":"civ", "Cabo_Verde":"cpv", "Cameroon":"cmr", "Central_African_Republic":"caf", "Chad":"tcd",
	"Comoros":"com", "Democratic_Republic_Congo":"cod", "Equatorial_Guinea":"gnq", "Gabon":"gab", "Gambia":"gmb", "Ghana":"gha", "Kenya":"ken",
	"Liberia":"lbr", "Madagascar":"mdg", "Malawi":"mwi", "Mali":"mli", "Mauritania":"mrt", "Mozambique":"moz", "Niger":"ner", "Nigeria":"nga",
	"Republic_of_Congo":"cog", "Rwanda":"rwa", "Sao_Tome":"stp", "Senegal":"sen", "Sierra_Leone":"sle", "Tanzania":"tza", "Togo":"tgo", "Uganda":"uga",
	"Zambia":"zmb", "Brazil":"bra", "Aruba":"abw", "Bahamas":"bhs", "Barbados":"brb", "Belize":"blz", "Bermuda":"bmu", "Caicos_Islands":"tca",
	"Costa_Rica":"cri", "Cuba":"cub", "Dominica":"dma", "Dominican_Republic":"dom", "El_Salvador":"slv", "Grenada":"grd", "Guatemala":"gtm",
	"Haiti":"hti", "Honduras":"hnd", "Jamaica":"jam", "Mexico":"mex", "Nicaragua":"nic", "Panama":"pan", "Puerto Rico":"pr", "Saint_Kitts_Nevis":"kna",
	"Saint_Lucia":"lca", "Saint_Vincent":"vct", "Trinidad_Tobago":"tto", "Virgin_Islands":"vgb", "Canada":"can", "United_States":"usa", "Argentina":"arg",
	"Bolivia":"bol", "Chile":"chl", "Colombia":"col", "Ecuador":"ecu", "Guyana":"guy", "Paraguay":"pry", "Peru":"per", "Suriname":"sur", "Uruguay":"ury",
	"Venezuela":"ven", "Afghanistan":"afg", "Armenia":"arm", "Azerbaijan":"aze", "Georgia":"geo", "Iran":"irn", "Kazakhstan":"kaz", "Kyrgyzstan":"kgz",
	"Tajikistan":"tjk", "Turkmenistan":"tkm", "Uzbekistan":"uzb", "China":"chn", "China":"cnn", "Hong_Kong":"hkg", "Japan":"jpn", "Macao":"mac", "Mongolia":"mng",
	"North_Korea":"prk", "South_Korea":"kor", "Bangladesh":"bgd", "Bhutan":"btn", "India":"ind", "Nepal":"npl", "Pakistan":"pak", "Seychelles":"syc",
	"Sri_Lanka":"lka", "Brunei ":"brn", "Cambodia":"khm", "Cook_Islands":"cok", "Federated_States_Micronesia":"fsm", "Fiji":"fji", "French_Polynesia":"pyf", "Indonesia":"idn",
	"Kiribati":"kir", "Lao":"lao", "Mauritius":"mus", "Myanmar":"mmr", "Nauru":"nru", "Palau":"plw", "Papua_New_Guinea":"png", "Philippines":"phl",
	"San_Marino":"smr", "Singapore":"sgp", "Solomon_Islands":"slb", "Thailand":"tha", "Timor-Leste":"tls", "Vanuatu":"vut", "Viet_Nam":"vnm", "Belarus":"blr",
	"Bosnia and Herzegovina":"bih", "Bulgaria":"bgr", "Croatia":"hrv", "Czechia":"cze", "Estonia":"est", "Hungary":"hun", "Latvia":"lva", "Lithuania":"ltu",
	"Moldova":"mda", "North_Macedonia":"mkd", "Poland":"pol", "Romania":"rou", "Serbia":"srb", "Slovakia":"svk", "Slovenia":"svn", "Ukraine":"ukr", "Taiwan": "twa",
	"Russia":"rus", "Albania":"alb", "Andorra":"and", "Austria":"aut", "Belgium":"bel", "Cyprus":"cyp", "Denmark":"dnk", "Finland":"fin", "France":"fra",
	"Germany":"deu", "Greece":"grc", "Greenland":"grl", "Iceland":"isl", "Ireland":"irl", "Italy":"ita", "Liechtenstein":"lic", "Luxembourg":"lux", "Maldives":"mdv",
	"Malta":"mlt", "Netherlands":"nld", "Norway":"nor", "Portugal":"prt", "Spain":"esp", "Sweden":"swe", "Switzerland":"che", "United_Kingdom":"gbr", "Bahrain":"bhr",
	"Egypt":"egy", "Iraq":"irq", "Israel":"isr", "Jordan":"jor", "Kuwait":"kwt", "Lebanon":"lbn", "Oman":"omn", "Palestine":"pse", "Qatar":"qat",
	"Saudi_Arabia":"sau", "Syria":"syr", "Turkey":"tur", "United_Arab_Emirates":"are", "Yemen":"yem", "Australia":"aus", "Malaysia":"mys", "Marshall_Islands ":"mhl",
	"New_Caledonia":"ncl", "New_Zealand":"nzl", "Samoa":"wsm", "Tonga":"ton", "Tuvalu":"tuv", "Namibia":"nam", "Norfolk_Island":"nfk", "Guam":"gum",
	"Northern_Mariana_Islands":"mnp", "Cayman_Islands":"cym", "Sint_Maarten":"none", "Guadeloupe":"none", "Martinique":"none", "Curaçao":"none",
	"Mayotte":"none", "Guinea-Bissau":"none", "Réunion":"none", "Côte_d'Ivoire":"civ", "French_Guiana":"guf", "Monaco":"none", "Jersey":"none", "The_Vatican":"none",
	"Åland":"none", "Faroe_Islands":"none", "Isle_of_Man":"none", "Gibraltar":"none", "American_Samoa":"none", "Christmas_Island":"none", "Wallis_Futuna":"none"}
		
	#Dictionary mapping between ISO-2 and ISO-3 language codes
	mapping_dict = {"aa":"aar", "ab":"abk", "ae":"ave", "af":"afr", "ak":"aka", "am":"amh", "an":"arg", "ar":"ara", "as":"asm", "av":"ava", "ay":"aym", "az":"aze", 
	"ba":"bak", "be":"bel", "bg":"bul", "bi":"bis", "bm":"bam", "bn":"ben", "bo":"bod", "bo":"bod", "br":"bre", "bs":"bos", "ca":"cat", "ce":"che", "ch":"cha", 
	"co":"cos", "cr":"cre", "cs":"ces", "cs":"ces", "cu":"chu", "cv":"chv", "cy":"cym", "cy":"cym", "da":"dan", "de":"deu", "de":"deu", "dv":"div", "dz":"dzo", 
	"ee":"ewe", "el":"ell", "el":"ell", "en":"eng", "eo":"epo", "es":"spa", "et":"est", "eu":"eus", "eu":"eus", "fa":"fas", "fa":"fas", "ff":"ful", "fi":"fin", 
	"fj":"fij", "fo":"fao", "fr":"fra", "fr":"fra", "fy":"fry", "ga":"gle", "gd":"gla", "gl":"glg", "gn":"grn", "gu":"guj", "gv":"glv", "ha":"hau", "he":"heb", 
	"hi":"hin", "ho":"hmo", "hr":"hrv", "ht":"hat", "hu":"hun", "hy":"hye", "hy":"hye", "hz":"her", "ia":"ina", "id":"ind", "ie":"ile", "ig":"ibo", "ii":"iii", 
	"ik":"ipk", "io":"ido", "is":"isl", "is":"isl", "it":"ita", "iu":"iku", "ja":"jpn", "jv":"jav", "ka":"kat", "ka":"kat", "kg":"kon", "ki":"kik", "kj":"kua", 
	"kk":"kaz", "kl":"kal", "km":"khm", "kn":"kan", "ko":"kor", "kr":"kau", "ks":"kas", "ku":"kur", "kv":"kom", "kw":"cor", "ky":"kir", "la":"lat", "lb":"ltz", 
	"lg":"lug", "li":"lim", "ln":"lin", "lo":"lao", "lt":"lit", "lu":"lub", "lv":"lav", "mg":"mlg", "mh":"mah", "mi":"mri", "mi":"mri", "mk":"mkd", "mk":"mkd", 
	"ml":"mal", "mn":"mon", "mr":"mar", "ms":"msa", "ms":"msa", "mt":"mlt", "my":"mya", "my":"mya", "na":"nau", "nb":"nob", "nd":"nde", "ne":"nep", "ng":"ndo", 
	"nl":"nld", "nl":"nld", "nn":"nno", "no":"nor", "nr":"nbl", "nv":"nav", "ny":"nya", "oc":"oci", "oj":"oji", "om":"orm", "or":"ori", "os":"oss", "pa":"pan", 
	"pi":"pli", "pl":"pol", "ps":"pus", "pt":"por", "qu":"que", "rm":"roh", "rn":"run", "ro":"ron", "ro":"ron", "ru":"rus", "rw":"kin", "sa":"san", "sc":"srd", 
	"sd":"snd", "se":"sme", "sg":"sag", "si":"sin", "sk":"slk", "sk":"slk", "sl":"slv", "sm":"smo", "sn":"sna", "so":"som", "sq":"sqi", "sq":"sqi", "sr":"srp", 
	"ss":"ssw", "st":"sot", "su":"sun", "sv":"swe", "sw":"swa", "ta":"tam", "te":"tel", "tg":"tgk", "th":"tha", "ti":"tir", "tk":"tuk", "tl":"tgl", "tn":"tsn", 
	"to":"ton", "tr":"tur", "ts":"tso", "tt":"tat", "tw":"twi", "ty":"tah", "ug":"uig", "uk":"ukr", "ur":"urd", "uz":"uzb", "ve":"ven", "vi":"vie", "vo":"vol", 
	"wa":"wln", "wo":"wol", "xh":"xho", "yi":"yid", "yo":"yor", "za":"zha", "zh":"zho", "zh":"zho", "zu":"zul", "un":"unk"}
	
	for lang in langs:
		for region in os.listdir("cglu"):
			if "." not in region:
						
				for country in os.listdir(os.path.join("cglu", region)):
					if "." not in country:
						
						try:
							nav_list = os.listdir(os.path.join("cglu", region, country))
						except Exception as e:
							print(e)
							nav_list = []
						
						for language in nav_list:						
							if "." not in language:
								if lang == language:
								
									new_lines = []
									files = []
									files = os.listdir(os.path.join("cglu", region, country, language))
									files = [os.path.join("cglu", region, country, language, x) for x in files if x.endswith(".gz")]
									random.shuffle(files)
									
									if len(files) > country_limit:
										files = random.sample(files, country_limit)
									country_name = iso_dict[country]
									os.makedirs(os.path.join(temp_folder, language), exist_ok = True)
									write_name = os.path.join(temp_folder, language, language + "." + country + ".INSERT.csv")
									
									if not os.path.isfile(write_name.replace("INSERT","1")):
										if country_name != "none":
										
											#print("\tStarting " + country_name + " with " + str(len(files)) + " files.")
											write_counter = 1
											
											for file in files:
												new_lines += process_file(file, language)
												
												if len(new_lines) > 5000000:
													new_df = pd.DataFrame(new_lines)
													new_df.columns = ["Text"]
													new_df.loc[:,"Country"] = country
													new_df.loc[:,"Language"] = language
													new_df.drop_duplicates(subset = "Text", keep = "first", inplace = True)
													new_df.to_csv(write_name.replace("INSERT",str(write_counter)))
													del new_df
													new_lines = []
													write_counter += 1
																			
															
											#Done with files
											if len(new_lines) > 1000:
												new_df = pd.DataFrame(new_lines)
												new_df.columns = ["Text"]
												new_df.loc[:,"Country"] = country
												new_df.loc[:,"Language"] = language
												new_df.drop_duplicates(subset = "Text", keep = "first", inplace = True)
												new_df.to_csv(write_name.replace("INSERT",str(write_counter)))
												#print("\t\t", write_name)
												del new_lines
												del new_df
											
		print("Done with " + lang)
		data = []
		file_counter = 1

		for file in sorted(list(os.listdir(os.path.join(".", temp_folder, lang)))):
			if True:
				print(file)
				df = pd.read_csv(os.path.join(".", temp_folder, lang, file))
				df.drop("Unnamed: 0", axis = "columns", inplace = True)
				data.append(df)
				del df
				
				mem = psutil.virtual_memory()[2]
				print(mem)
				
				if mem > 40.0:
					print("\n\nSaving batch.")
					data = pd.concat(data)
					start = len(data)
					data.drop_duplicates(subset = "Text", keep = False, inplace = True)
					print(start, len(data))

					print("\nAdding number of words.")
					words_list = []
					for row in data.itertuples(index = False, name = None):
							line = row[0].split()
							words_list.append(len(line))

					data.loc[:,"N_Words"] = words_list
					print("Total words = ", end = " ")
					print(str(sum(words_list)))
					
					data.to_csv("GeoCorpus." + lang + "." + str(file_counter) + ".gz", compression = "gzip")
					file_counter += 1
					del data
					data = []
					gc.collect()
				
		#Done
		print("\n\nFinished loading all files.")
		data = pd.concat(data)
		start = len(data)
		data.drop_duplicates(subset = "Text", keep = False, inplace = True)
		print(start, len(data))

		print("\nAdding number of words.")
		words_list = []
		for row in data.itertuples(index = False, name = None):
				line = row[0].split()
				words_list.append(len(line))

		data.loc[:,"N_Words"] = words_list
		print("Total words = ", end = " ")
		print(str(sum(words_list)))
		
		data.to_csv("GeoCorpus." + lang + "." + str(file_counter) + ".gz", compression = "gzip")
		del data
		
		#Clean
		for file in os.listdir(os.path.join(".", temp_folder, lang)):
			os.remove(os.path.join(".", temp_folder, lang, file))
			
		gc.collect()