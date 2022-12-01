import tqdm
from common_crawl_corpus import WET_processor

from common_crawl_corpus import common_crawl_processing



with open("wet.paths") as wet_files:
    lines = wet_files.readlines()
    for line in tqdm.tqdm(lines):
        print(line)
        common_crawl_processing.download_wet(line)
