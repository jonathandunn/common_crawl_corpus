from tqdm import tqdm
import pandas as pd
import glob
import time

tqdm.pandas(ncols=70)

"""You will need to download the segment from the first index (idealy around 250 instance) and used WET processor to 
process and save the dataframe with name processed... """
concats = []
top_files = glob.glob("/Volumes/nathan/Common Crawl/processed*")[:250]

result = []

for i, filename in enumerate(tqdm(top_files)):
    if i > 100 and i % 3 != 0:
        continue
    temp_df = pd.read_pickle(filename, compression="gzip")
    temp_df["Hash"] = temp_df["Text"].apply(hash)

    concats.append(temp_df)
    df = pd.concat(concats, ignore_index=True)

    start = time.time()
    unhashed_df = df.drop_duplicates(subset="Text", inplace=False, ignore_index=True)
    total_time_unhashed = time.time() - start

    start = time.time()
    hashed_df = df.drop_duplicates(subset="Hash", inplace=False, ignore_index=True)
    total_time_hashed = time.time() - start

    matched = len(unhashed_df.index) - len(hashed_df.index)

    result.append([i, total_time_unhashed, total_time_hashed, matched])

    df = pd.DataFrame(result, columns=["Segments", "Unhashed dedup", "Hashed dedup", "Mismatched Count"])
    df.to_csv("/Volumes/nathan/Common Crawl/all_perf.csv")
    df.to_feather("/Volumes/nathan/Common Crawl/all_perf.feather")

# df.to_feather("/Volumes/nathan/Common Crawl/all_raw.feather")
