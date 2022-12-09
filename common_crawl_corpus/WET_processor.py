import re
import warcio
import pandas as pd
from cytoolz import pipe, juxt
from gensim.parsing import preprocessing
from . import utilities
from typing import Optional, List, Tuple
from alphabet_detector import AlphabetDetector
import collections
import logging

alphabet_detector = AlphabetDetector()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.debug('call to WET_processer')






def read_wet(file_dir: str) -> pd.DataFrame:
    """Save processed WET_record to list then save it to pickle file"""
    # TODO: add multiprocessing for wet not wet record
    logger.debug('Inside read_wet, directory is set as %s', str(file_dir))
    logger.info('Reading wet file')
    with open(file_dir, "rb") as file:
        lines = []
        for record in warcio.ArchiveIterator(file):
            if temp := extract_wet_record(record):
                lines.extend(temp)

        df = pd.DataFrame(lines)
        deduplicate(df)
        return df
        # save_df(df, filename=filename.replace("/", ".") + ".processed")


def extract_wet_record(wrac_record) -> Optional[List[Tuple[str, str, str, int, str]]]:
    """Process individual WRAC record in WET file, return list ofi
    url_suffix, current_country, current_region, url, line"""
    if wrac_record.rec_type != "conversion":
        return
    url: str = wrac_record.rec_headers.get_header("WARC-Target-URI")
    logger.debug('inside extract_wet_record: url of record is %s', str(url))
    # getting domain abc.example.com -> ExtractResult(subdomain='abc', domain='hostname', suffix='com')
    url_domain, url_suffix = utilities.extract_url(url)

    # TODO There are bugs where the tldextract url of trademe.co.nz would have the suffix of 'co.nz'
    if url_suffix not in utilities.COUNTRY_CODES_NAME.keys() or url_domain in utilities.URL_FILTER:
        return
    current_country = utilities.COUNTRY_CODES_NAME.get(url_suffix)

    web_content: str = wrac_record.content_stream().read().decode("utf-8")
    # we need the line larger than 15 character
    processed_line: List[Tuple[str, str, str, int, str]] = []
    line_num = 0  # flag to make sure it is the same page
    for line in web_content.splitlines():
        if len(line) <= 15:
            continue
        # Remove links, hashtags, at-mentions, mark-up, and "RT"
        line = re.sub(r"http\S+", "", line)
        line = re.sub(r"@\S+", "", line)
        line = re.sub(r"#\S+", "", line)
        line = re.sub("<[^>]*>", "", line)

        # Remove emojis
        line = utilities.remove_emoji(line)

        # Remove extra spaces
        line = pipe(line,
                    preprocessing.strip_tags, preprocessing.split_alphanum, preprocessing.strip_multiple_whitespaces)
        # Check if still above 15 and not contains any navigational / boilerplate characters
        if len(line) <= 15 or any(char in line for char in utilities.ILLEGAL_CHAR):
            continue
        # Check if mostly numbers / characters
        character_only: str = pipe(line, preprocessing.strip_numeric, preprocessing.strip_punctuation)
        if len(character_only) <= 12:
            continue
        # Check if line has Chinese / Japanese / Korean characters, then set length to 15:
        if any(juxt(alphabet_detector.is_cjk,
                    alphabet_detector.is_hangul,
                    alphabet_detector.is_hiragana,
                    alphabet_detector.is_katakana
                    )(line)):
            length = 15
        else:
            length = 50
        if len(line) < length:
            continue
        string_counter = collections.Counter(line)
        if all([string_counter.get("-", 0) < 4, string_counter.get("(", 0) < 4, string_counter.get(")", 0) < 4,
                string_counter.get("=", 0) < 2, string_counter.get("_", 0) < 2, string_counter.get(".", 0) < 15,
                string_counter.get("&", 0) < 4, string_counter.get("[", 0) < 3, string_counter.get("]", 0) < 3,
                string_counter.get("*", 0) < 5]):
            line_num += 1
            processed_line.append((url_suffix, current_country, url, line_num, line))
    return processed_line


def deduplicate(df: pd.DataFrame):
    # TODO This deduplication only consider exact duplicate, which is not usually applicable in practice,
    #  maybe looking in to Levenshtein distance: TheFuzz, jellyfish or difflib.SequenceMatcher
    df.columns = ("Domain", "Country", "URL", "LineID", "Text")
    original_len = len(df.index)
    df.drop_duplicates(subset="Text", inplace=True, ignore_index=True)
    logger.info('inside deduplicate')
    logger.debug(f"Formatted and Removed {original_len - len(df.index)} with remaining: {len(df.index)}")

def drop_mnc_url(df: pd.DataFrame):
    #This function drops the URL based on the list of known international companies provided earlier. Specify
    #your own or build better tools.
    logger.info('inside drop_mnc_url: dropping urls of mncs')
    df.columns = ("Domain", "Country", "URL", "LineID", "Text")
    original_len = len(df.index)
    #for each url, where domain is a member of set of URLs
    print(f"Removed {original_len - len(df.index)} with remaining: {len(df.index)}")
    raise NotImplementedError

class Deduplicator():
    """This class exists over the lifecycle of a processing run in order to 
    hold statistics, hashstate for record elimintion between shards and prevent 
    costly rereading of url lists into the system. It has three main methods, 
    deduplicate_exact, deduplicate_cluster, deduplicate_international 
    deduplicate exact removes the exact matching records,
    deduplicate clusters removes the clusters LSA matching records,
    dedupliate international removes multinational websites from the records"""
