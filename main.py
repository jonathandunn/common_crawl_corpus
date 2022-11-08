import tqdm
from common_crawl_corpus import WET_processor

from common_crawl_corpus import common_crawl_processing

WET_processor.read_wet("CC-MAIN-20220924151538-20220924181538-00000.warc.wet")

with open("wet.paths") as wet_files:
    lines = wet_files.readlines()
    for line in tqdm.tqdm(lines):
        common_crawl_processing.download_wet(line)
