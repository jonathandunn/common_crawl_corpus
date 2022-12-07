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
import logging
#---------------------

def process_lid(segment, input_dir, output_dir):

    #Check if file has been processed
    check = segment.replace("/",".").replace(".hdf",".txt")
    
    if check not in list(os.listdir(os.path.join(".", "check"))):
    
        print("Starting " + segment)
        from lid.lidNet.lidNet import lidNet
        lid = lidNet(os.path.join("lid", "lidNet", "Models", "Model.LID.MLP.400kx3_hash.1-3grams.262k.hdf"))
    
        #Load and prepare
        current_df = pd.read_hdf(segment, key = "data")
        
        #Get meta-data
        meta = current_df.iloc[1,]
        current_country = meta["Country"]
        current_region = meta["Region"]

        #Get time
        section = segment.split(".")[2:]
        current_time = ".".join(section).replace(".hdf","").replace("CC-MAIN-","")
        current_time_write = current_time
        current_time = current_time[:7]
        
        text_list = []  #Initialize
        
        #Join texts by webpage
        for section in current_df.groupby(by = "URL"):

            current_url = str(section[0])
            text = section[1].Text.values
            text = str("\n".join(text))
            current_size = len(text.split())
            
            text_list += [(current_time, current_url, current_size, text)]

        current_df = pd.DataFrame(text_list, columns = ["Time", "URL", "N_Words", "Text"])
        current_df.loc[:,"Language"] = lid.predict(list(current_df.loc[:,"Text"].values))

        for section in current_df.groupby(by = "Language"):

            current_lang = str(section[0])
            write_name = current_region + "." + current_country + "." + current_lang + "." + current_time_write
            os.makedirs(os.path.join(output_dir, current_region, current_country, current_lang), exist_ok = True)
            write_name = os.path.join(output_dir, current_region, current_country, current_lang, write_name)
            section = section[1]
            section.to_csv(write_name + ".gz", header = True, index = False, index_label = False, compression = "gzip")
        
        #Done with all langs
        with open(os.path.join("check", check), "w") as fo:
            fo.write("Done")
            
        os.remove(segment)
        print("\tDeleted " + segment)
            
        return
#--------------------

class CC_Corpus(object):

    def __init__(self, countries_to_skip = []):
    
        #Ignore certain countries if there is already enough data
        self.countries_to_skip = countries_to_skip
    
        #This list defines what countries to include in the corpus
        self.country_codes = []

        #This dictionary maps country codes to (English) country names
        self.country_names = {}
    
        #This dictionary maps country names to large regions for organizational purposes
        self.country_regions = {}
            
        #this sets up our module level logging so we can track execution
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.DEBUG)
        self.formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.ch.setFormatter(self.formatter)
        self.logger.addHandler(self.ch)
        self.logger.debug('cc_corpus class initialized')
    #----------------------------------------------------------------------------------------------#
    
    def strip_tags(self, line):
        self.logger.debug("stripping tags in strip_tags")
        line = re.sub(r"http\S+", "", line)
        line = re.sub("<[^>]*>", '', line)
        
        return line
    #----------------------------------------------------------------------------------------------#

    def process_wet(self, file, read_bucket):
        self.logger.debug('starting to process wet file named %s', file)
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
            
            self.logger.info('ending processing of wet file: %s', str(file))
            self.logger.debug('loading ' + str(file) + ': ' + str(time.time() - starting))
            return line_list
            
        except Exception as e:
            self.logger.error(e)
            self.logger.error('processing of wet file aborted due to error: %s', str(file))
            
            return []
    #------------------------------------------------------------------------------------------------#

    def crawl_cc(self, prefix_list, write_bucket, workers = 1):
        self.logger.info('beginning crawl_cc function') 
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
                    country_done_list = self.countries_to_skip
                    
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
                    
                    current_count = 0   #For counting millions of samples
                        
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
    
    def lid_cc(self, input_dir, output_dir, region, workers):
    
        segment_list = []
        for root, dirs, files in os.walk(os.path.join(input_dir, region)):
            for file in files:
                file = os.path.join(root, file)
                segment_list.append(file)
            
        #Multi-process by file
        pool_instance = mp.Pool(processes = workers, maxtasksperchild = 1)
        line_list = pool_instance.map(partial(process_lid,
                                                    input_dir = input_dir,
                                                    output_dir = output_dir
                                                    ), segment_list, chunksize = 1)

        pool_instance.close()
        pool_instance.join()
                    
    #--------------------------------------------------------------------
    
    def final_cc(self, input_dir, output_dir, region):
    
        for country in os.listdir(os.path.join(input_dir, region)):
            for language in os.listdir(os.path.join(input_dir, region, country)):
                
                first_flag = True   #First for this set
                counter = 1
                
                for file in os.listdir(os.path.join(input_dir, region, country, language)):
                
                    file = os.path.join(input_dir, region, country, language, file)
                    new_df = pd.read_csv(file, compression = "gzip")
                    
                    if first_flag == True:
                        first_flag = False
                        current_df = new_df
                        print("\tFirst time for " + region + " " + country + " " + language)
                        
                    else:
                        
                        #First, merge new_df
                        print("\tContinuing with " + file)
                        current_df = pd.concat([current_df, new_df])
                        current_df.drop_duplicates(subset = "URL", keep = "first", inplace = False)
                        
                        #Second, check length
                        if len(current_df) > 100000:
                            write_df = current_df.head(n = 100000)
                            current_df = current_df.tail(n = len(current_df) - 100000)
                            
                            write_name = region + "." + country + "." + language + "." + str(counter) + ".gz"
                            write_name = os.path.join(output_dir, region, country, language, write_name)
                            os.makedirs(os.path.join(output_dir, region, country, language), exist_ok = True)
                            counter += 1
                            
                            write_df.to_csv(write_name, header = True, index = False, index_label = False, compression = "gzip")
                            print("\t\tWriting " + write_name)
                            del write_df
                            
                #Done with all files, write the remaining
                write_name = region + "." + country + "." + language + "." + str(counter) + ".gz"
                write_name = os.path.join(output_dir, region, country, language, write_name)
                os.makedirs(os.path.join(output_dir, region, country, language), exist_ok = True)
                counter += 1
                            
                current_df.to_csv(write_name, header = True, index = False, index_label = False, compression = "gzip")
                print("\t\tWriting " + write_name)
                
                del current_df
                
                #Done, now delete
                for file in os.listdir(os.path.join(input_dir, region, country, language)):
                
                    file = os.path.join(input_dir, region, country, language, file)
                    os.remove(file)                 
    
    #--------------------------------------------------------------------
