#TODO: Before running this script please make sure that there are json files with scraped school data in directory 'jedeschule-scraper/data'
#TODO: Before running this script please make sure that there is one json file with shl school data in directory 'jedeschule-scraper/shl/data/in/shl_data.json'

import fuzzywuzzy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import numpy as np
import utils
import json
import datetime
import os
from pathlib import Path

# initialize logger
log = utils.set_logging()

log.info('Loading input data')
### loading scraped schools
df_scrape = utils.load_scraped_data()

### loading shl schools
df_shl_in, df_shl = utils.load_shl_data()

### grab relevant columns for fuzzy matching
df_scrape_fuzzy = df_scrape[['id_scraped', 'name_scraped', 'anschrift_scraped', 'plz_scraped', 'ort_scraped']]
df_shl_fuzzy = df_shl[['id', 'name', 'anschrift', 'plz', 'ort']]

# Idea is to only compare schools whose plz is in shl data for better performance but reduces schools by a few only...
plz_shl_unique = df_shl.plz.unique()
df_scrape_fuzzy = df_scrape_fuzzy[df_scrape_fuzzy.plz_scraped.isin(plz_shl_unique)]

starttime = datetime.datetime.now()

### fuzzy matching
best_matches = []
schools_total = len(df_shl_fuzzy.index) # for logging only
log.info('Start fuzzy matching')
# iterating over all shl schools
for index, row in df_shl_fuzzy.iterrows():
    # for logging only (log every 10 schools)
    if (index+1)%10 == 0:
        log.info('School ' + str(index+1) + ' of ' + str(schools_total))
    # initializing variables
    id_scraped_best = -1
    score_best = -1
    score_name_best = -1
    score_anschrift_best = -1
    # iterating over all scraped schools for every shl school
    for index_scraped, row_scraped in df_scrape_fuzzy.iterrows():
        # let's only compare in case of same plz (halves the runtime)
        # possible TODO: can also be done via fuzzy score of "ort" if plz can't be trusted
        if row['plz'] == row_scraped['plz_scraped']:
            # score = fuzz.token_set_ratio(row['compare_string'].casefold(), row_scrape['compare_string_scrape'].casefold())
            # calculating scores via fuzz library
            score_name = fuzz.token_set_ratio(row['name'].casefold(), row_scraped['name_scraped'].casefold())
            score_anschrift = fuzz.token_set_ratio(row['anschrift'].casefold(), row_scraped['anschrift_scraped'].casefold())
            score = (score_name + score_anschrift)*0.5 # combined score of the compared values
            # assign values of school in terms of better matching
            if score > score_best:
                id_scraped_best = row_scraped['id_scraped']
                score_best = score
                score_name_best = score_name
                score_anschrift_best = score_anschrift
            if score == 100:
                break # leave for loop early if school matches perfectly
    # store best matched school
    best_match = [row['id'], id_scraped_best, score_best, score_name_best, score_anschrift_best]
    best_matches.append(best_match)

endtime = datetime.datetime.now()

log.info('Dumping data')
### joining data and writing to output files
# writing all shl schools with the data of scraped schools and scores to csv, but only columns which were used for matching
df_best_matches = pd.DataFrame(best_matches, columns=['id', 'id_scraped', 'score', 'score_name', 'score_anschrift'])
matching_full_data = pd.merge(df_shl_in, df_best_matches, on=['id'], how='inner')
matching_full_data = pd.merge(matching_full_data, df_scrape, on=['id_scraped'], how='inner')
matching_full_data = matching_full_data[['id', 'id_scraped', 'score', 'score_name', 'name', 'name_scraped', 'score_anschrift', 'anschrift', 'anschrift_scraped', 'ort', 'ort_scraped']]
path_out = Path(__file__) / "../data/matching_full_data.csv"
matching_full_data.to_csv(path_out, index=False, sep=';')

# enrich shl data with id of best match in scraped schools and score and dumping to json
# TODO: Not merging the best match for EVERY school but only when best match score was above a defined value
df_best_matches = df_best_matches[['id', 'id_scraped', 'score']]
matching_shl_data_enriched = pd.merge(df_shl_in, df_best_matches, on=['id'], how='left')
path_out = Path(__file__) / "../data/matching_shl_data_enriched.json"
matching_shl_data_enriched.to_json(path_out, orient='records')

# calculating matching_stats (Start/Endtime, Schools total, perfect matches etc.)
perfect = similar = likely = unlikely = no = rest = 0
for val in df_best_matches['score']:
    if int(val) == 100:
        perfect += 1
    elif int(val) == -1:
        no += 1
    elif int(val) < 100 and int(val) >= 90:
        similar += 1
    elif int(val) < 90 and int(val) >= 80:
        likely +=1
    elif int(val) < 80 and int(val) >= 70:
        unlikely += 1
    else:
        rest += 1

total_schools   = len(df_best_matches)
perfect_perc    = perfect/total_schools
similar_perc    = similar/total_schools
likely_perc     = likely/total_schools
unlikely_perc   = unlikely/total_schools
no_perc         = no/total_schools
rest_perc       = rest/total_schools

runtime = endtime - starttime

stats = [
['Runtime', runtime, ''],
['totalSchools', total_schools, ''],
['perfectMatch', perfect, perfect_perc],
['similarMatch', similar, similar_perc],
['likelyMatch', likely, likely_perc],
['unlikelyMatch', unlikely, unlikely_perc],
['noMatch', no, no_perc],
['Rest', rest, rest_perc]
]

df_stats = pd.DataFrame(stats)
path_out = Path(__file__) / "../data/matching_stats.csv"
df_stats.to_csv(path_out, index=False, sep=';', header=False)

log.info('Done')