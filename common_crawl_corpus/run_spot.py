from cc_corpus import CC_Corpus
import random

#Initialize
cc = CC_Corpus()

#Define the temp s3 bucket
write_bucket = "ccglu1"

#Define prefix from the Common Crawl
prefix_list = [
		"CC-MAIN-2013-20", 
		"CC-MAIN-2013-48", 
		"CC-MAIN-2014-10", 
		"CC-MAIN-2014-15",
		"CC-MAIN-2014-23", 
		"CC-MAIN-2014-35", 
		"CC-MAIN-2014-41", 
		"CC-MAIN-2014-42",
		"CC-MAIN-2014-49", 
		"CC-MAIN-2014-52", 
		 ]
		 
#Choose random segment
random.shuffle(prefix_list)

cc.crawl_cc(prefix_list, write_bucket, workers = 12)

#cc.lid_cc(input_dir, "./Output", lid_model)