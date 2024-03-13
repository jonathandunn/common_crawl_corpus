import os
import random
import codecs
import tldextract
import pickle
import pandas as pd
import numpy as np
import cytoolz as ct
import multiprocessing as mp
from functools import partial
from corpus_similarity.corpus_similarity import Similarity
import hashlib
from typing import Iterable, Iterator, Sequence, Sized, Tuple, Type

HASH_TYPE: Type[np.uint64] = np.uint64
HASH_SIZE = HASH_TYPE(0).nbytes

#---------------------------------------------------------------
def aggregate(df, chunksize = 5000):

    #Sort to ensure all websites are grouped
    df.sort_values("URL", inplace=True)
    
    #Holder for savin aggregated documents
    samples = []   
    current_url = False
    current_count = 0
    current_text = ""

    #Iterate over chunks
    for row in df.itertuples():
        
        #Get data
        date = row[1]
        url = row[2]
        n_words = row[3]
        whole_text = row[4]
        
        #Find the top-level domain
        result = tldextract.extract(url)
        domain = result[1]
        code = result[2]
        
        #Look at smaller bits
        for text in whole_text.split("\n"):

            text = text.replace("\r", "")
            #Only join within the same url
            if current_url != False:
                if domain == current_url:
                
                    #Append new text
                    current_text += text + " "
                    
                    #Update counts
                    current_count += len(text.split())
                    
                #New webiste, stop joining
                elif domain != current_url:
                    samples.append([date, domain, current_count, current_text.strip()])
                    current_text = text
                    current_count = len(text.split())
                    current_url = domain                
                    
            #If this is the first new sample after a join    
            if current_url == False:
                current_url = domain
                current_count = len(text.split())
                current_text = text
                
            #Check if this is sufficient for a sample
            if current_count > chunksize:
            
                samples.append([date, domain, current_count, current_text])           
                current_text = ""
                current_count = 0
                current_url = False

    #Done going through corpus
    new_df = pd.DataFrame(samples)
    
    if len(new_df) > 10:
        new_df.columns = ["Date", "Domain", "N_Words", "Text"]
    
    return new_df  
 
#------------------------------------------------------------------
def _b2i(b):
    return np.frombuffer(b[:HASH_SIZE], dtype=HASH_TYPE, count=1, offset=0).item(0)
#------------------------------------------------------------------
    
def str_hash(s):
    h = hashlib.sha1(bytes(s, encoding="utf-8"))
    return _b2i(h.digest())

#------------------------------------------------------------------
def deduplicate(df):

    if "Hash" not in df.columns:
        df.loc[:,"Hash"] = [str_hash(s) for s in df.loc[:,"Text"].values]
    
    df.drop_duplicates(subset="Hash",keep=False, inplace=True, ignore_index=True)
    
    return df
    
#---------------------------------------------------------------