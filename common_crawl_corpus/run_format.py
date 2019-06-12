from cc_corpus import CC_Corpus

nickname = str(input("Type segment name:  "))
nickname = str(nickname)

#Initialize
cc = CC_Corpus()
	
#Run
cc.format_cc(nickname, "ccglu1", "ccglu2")