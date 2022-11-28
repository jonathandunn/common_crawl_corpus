import os
import codecs
import pandas as pd
import cytoolz as ct
import multiprocessing as mp
from functools import partial
from urllib.parse import urlparse
import tldextract
import pickle

url_filter = []

			        
urls = df.loc[:,"URL"].values
			        
for url in urls:
			        
	result = tldextract.extract(url)
	domain = result[1]
	code = result[2]
				
	if len(code.split(".")) > 1:
		code = code.split(".")
		code = code[-1]
				

   #domain = primary web site
   #code = country code
