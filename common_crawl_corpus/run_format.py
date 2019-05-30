from cc_corpus import CC_Corpus

#Initialize
cc = CC_Corpus()

nicknames = [
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
			
#Run
for nickname in nicknames:
	cc.format_cc(nickname, "ccglu1", "ccglu2")