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
import re
import string
import shutil
#from corpus_similarity.corpus_similarity import Similarity

from gensim.models import FastText
from gensim.parsing import preprocessing 
from gensim.models.fasttext import load_facebook_model
from gensim.models.fasttext import save_facebook_model
import multiprocessing as mp
import scipy

from matplotlib import pyplot as plt

# all non alphanumeric
symbols = re.compile(r'(\W+)', re.U)
# single character removal
punctuation = re.compile(r'([%s])+' % re.escape(string.punctuation), re.UNICODE)
# separators (any whitespace)
seps = re.compile(r'\s+')
#Uppercase merged words
uppercase = re.compile(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))')

CUSTOM_FILTERS = [lambda x: x.lower(), preprocessing.strip_tags, preprocessing.strip_punctuation, preprocessing.strip_multiple_whitespaces, preprocessing.strip_numeric]

#cleaner (order matters)
def clean(text): 
    
    text = symbols.sub(r' \1 ', text)
    text = punctuation.sub(r' \1 ', text)
    #text = uppercase.sub(r'\1 ', text)
    #text = preprocessing.strip_multiple_whitespaces(text)
    
    return text
    
#-------------------
class Sentences(object):

    def __init__(self, df):
        self.df = df
 
    def __iter__(self):
    
        for text in self.df.loc[:,"Text"].values:
            yield preprocessing.preprocess_string(text, CUSTOM_FILTERS)

#---------------
def train_model(df, window=5, sg=1, region="Region", country="Country", language="Language"):

    input_data = Sentences(df)
    output_file = "model."+region+"."+country+"."+language+".bin"
    
    if not os.path.exists(output_file):
    
            model = FastText(vector_size=100, window=window, sg=sg, hs=1, sorted_vocab=1, alpha=0.01, min_count = 5, workers=32)
            model.build_vocab(corpus_iterable=input_data)
            model.train(corpus_iterable=input_data, total_examples=len(df), epochs = 2, queue_factor = 4)
            save_facebook_model(model, output_file)

    else:
        print("Loading ", output_file)
        model = load_facebook_model(output_file)    
    
    return model
    
#-----------------
def get_perplexity(df, model, region, country, language, id, figure = True):

    print("Getting perplexity")
    input_data = Sentences(df)
    output = model.score(sentences=input_data, total_sentences=len(df), chunksize=10, queue_factor=4)
    
    df.loc[:,"Probability"] = output
    df.loc[:,"Perplexity"] = df.loc[:,"Probability"].divide(df.loc[:,"N_Words"])
    before = len(df)
    
    if figure == True:
        scipy.stats.probplot(df.loc[:,"Perplexity"], dist='norm', fit=True, plot = plt)
        plt.savefig("perplexity.original."+region+"."+country+"."+language+"." + str(id) + ".png")
        plt.clf()
    
    return df
    
#-----------------
def seq_outliers(df, model, region, country, language, id):
    
    #Get Iglewicz and Hoaglin modified Z-score
    mad = scipy.stats.median_abs_deviation(df.loc[:, "Perplexity"])
    median = np.median(df.loc[:, "Perplexity"])
    scores = [(0.6745*(x-median))/mad for x in df.loc[:,"Perplexity"]]

    df.loc[:,"Score"] = scores
    df.loc[:,"Prediction"] = "IN"

    #Get samples too high (i.e., too predictable)
    mask = df["Score"] > 3
    df.loc[mask, "Prediction"] = "HIGH"
    high_cut = np.count_nonzero(mask)
    
    #Get samples too low (i.e., too random)
    mask = df["Score"] < -3
    df.loc[mask, "Prediction"] = "LOW"
    low_cut = np.count_nonzero(mask)
    print(region, country, language, "Too high ", high_cut, "Too low", low_cut)
    
    mask = df["Prediction"] == "IN"
    scipy.stats.probplot(df.loc[mask,"Perplexity"], dist='norm', fit=True, plot = plt)
    plt.savefig("perplexity.cleaned."+region+"."+country+"."+language+"."+str(id)+".png")
    plt.clf()

    df.drop(["Perplexity", "Probability"], inplace=True, axis=1)
    
    return df    

#---------------------------------------------------------------
def process_file(test_file, sample_size=1000):

    #Unpack the input
    region = test_file[0]
    country = test_file[1]
    language = test_file[2]
    num = test_file[3]
    file = test_file[4]
    
    #Only use cleaned files
    if ".clean." in file:
    
        #Load and aggregate into samples
        print(file)
        df = pd.read_csv(os.path.join(PATH_TO_CCGLU, region, country, language, file), index_col = 0)
        df.reset_index(drop=True, inplace=True)
        print("\tNumber of words ", df.loc[:,"N_Words"].sum(), file)
        os.makedirs(os.path.join("..", "CGLU_TWGLU", "CGLU_Outliers", region, country, language), exist_ok = True)
        original_length = df.loc[:,"N_Words"].sum()

        #Need a million words
        if original_length < 5000000:
            print("Need at least 5 million words", file)
            shutil.copyfile(os.path.join(PATH_TO_CCGLU, region, country, language, file), os.path.join("..", "CGLU_TWGLU", "CGLU_Outliers", region, country, language, file.replace(".gz",".original.gz")))
            
        else:
        
            if not os.path.exists(os.path.join("..", "CGLU_TWGLU", "CGLU_Outliers", region, country, language, region+"."+country+"."+language+"."+str(num)+".IN.gz")):

                window = 5
                sg = 1
                
                #Train model for outlier detection
                print("Training model for", region, country, language)
                model = train_model(df, window, sg, region, country, language)
                    
                #Get perplexity for actual corpus
                df = get_perplexity(df, model, region, country, language, num, figure = True)
                            
                #Outlier detection
                df = seq_outliers(df, model, region, country, language, num)
                print(df)
                
                df[df.loc[:,"Prediction"] == "IN"].to_csv(os.path.join("..", "CGLU_TWGLU", "CGLU_Outliers", region, country, language, region+"."+country+"."+language+"."+str(num)+".IN.gz"), compression="gzip")
                df[df.loc[:,"Prediction"].isin(["HIGH", "LOW"])].to_csv(os.path.join("..", "CGLU_TWGLU", "CGLU_Outliers", region, country, language, region+"."+country+"."+language+"."+str(num)+".OUT.gz"), compression = "gzip")
                ending_length = df[df.loc[:,"Prediction"] == "IN"].loc[:,"N_Words"].sum()
                print(region, country, language, num, "Before: ", original_length, "After: ", ending_length)
#---------------------------------------------------------------