from cc_corpus import CC_Corpus

nicknames = [
			"CC-MAIN-2014-35",
			"CC-MAIN-2014-41",
			"CC-MAIN-2014-42",
			"CC-MAIN-2014-49",
			"CC-MAIN-2014-52",
			]
			
#Run
for nickname in nicknames:

	#Initialize
	cc = CC_Corpus()
	
	#Run
	cc.format_cc(nickname, "ccglu1", "ccglu2")