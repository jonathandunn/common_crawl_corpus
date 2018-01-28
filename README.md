# common_crawl_corpus

Scripts for building a geo-located web corpus using Common Crawl data. This modules processes segments from the Common Crawl dataset (http://commoncrawl.org/) and then cleans, dedupes, organizes, and runs lidNet on the data.

Import the CC_Corpus class

	from common_crawl_corpus.cc_corpus import CC_Corpus

Initialize the CC_Corpus object

	CC_Corpus = CC_Corpus()
	
Define which segments of the Common Crawl to process

	prefix_list = ["CC-MAIN-2017-04", "CC-MAIN-2017-09", "CC-MAIN-2017-13"]
	
Begin crawling; results will be saved to the specified S3 bucket. Credentials must be available via the AWS-CLI

	CC_Corpus.crawl_cc(prefix_list, "Your_S3_Bucket", workers = 8)

Merge and dedupe processed crawl files after saving to local drive

	CC_Corpus.format_cc("CC-Main-2017-04", "./Data/Input/", "./Data/Output/")

Optionally, use lidNet to identify languages in the corpus. This process downloads deduped corpus segments from one S3 bucket, runs lidNet, and saves to an S3 bucket

	prefix_list = ["2016-18/", "2016-22/", "2016-26/", "2016-30/"]
	lid_model = "Model.LID.464langs.50chars.130k_hash.3-3.Callback.hdf"

	lid_cc("Your_Input_Bucket", "Your_Output_Bucket", prefix_list, lid_model)
