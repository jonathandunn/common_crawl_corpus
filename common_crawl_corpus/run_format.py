from cc_corpus import CC_Corpus

nickname = str(input("Type segment name:  "))
nickname = "CC-MAIN-" + str(nickname)

n_segments = str(input("Type number of segments per dedupe:  "))
n_segments = int(n_segments)

#Initialize
cc = CC_Corpus()
	
#Run
cc.format_cc(nickname, "ccglu1", "ccglu2", n_segments)