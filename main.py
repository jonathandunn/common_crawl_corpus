import tqdm
from common_crawl_corpus import WET_processor

from common_crawl_corpus import common_crawl_processing



with open("wet.paths") as wet_files:
    lines = wet_files.readlines()
    #common_crawl_processing.download_wet(lines[0])


value = WET_processor.read_wet('/home/james/common_crawl_corpus/cc_data/crawl-data-CC-MAIN-2022-40-segments-1664030331677.90-wet-CC-MAIN-20220924151538-20220924181538-00000.warc.wet.gz')
print(value)
