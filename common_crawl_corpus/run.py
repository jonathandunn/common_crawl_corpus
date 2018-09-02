lid_model = "Model.LID.MLP.400kx3_hash.1-3grams.262k.hdf"
input_dir = "./africa_north"

from cc_corpus import CC_Corpus
cc = CC_Corpus()

cc.lid_cc(input_dir, "./Output", lid_model)