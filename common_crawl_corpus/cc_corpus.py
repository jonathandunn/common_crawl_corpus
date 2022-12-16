import gzip
import tldextract
import codecs
import time
import re
import cytoolz as ct
import pandas as pd
import numpy as np
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
        self.logger.info('inside strip_tags')
        self.logger.debug("stripping tags in strip_tags")
        line = re.sub(r"http\S+", "", line)
        line = re.sub("<[^>]*>", '', line)
        
        return line
    #----------------------------------------------------------------------------------------------#

    def process_wet(self, file):
        """This method processes a single wet file and returns a dataframe containing the common fields"""
	raise NotImplementedError
    #------------------------------------------------------------------------------------------------#

    def crawl_cc(self, prefix_list):
	"""This method downloads the complete CC for a given prefix, from the path file to the WARC files."""
        raise NotImplementedError
                                                        
#----------------------------------------------------------------------------------------------------------------------#

    def format_cc(self, nickname, path_to_input, path_to_output):
        """This method conducts deduplication on a directory of crawl files, nicknanme is the CC instance you want to Dedupe"""
	raise NotImplementedError 
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
