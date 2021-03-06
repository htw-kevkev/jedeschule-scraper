import utils
import json
import os
from pathlib import Path
import pandas as pd
import datetime

# initialize logger
log = utils.set_logging()

starttime = datetime.datetime.now()

log.info('Loading input data')
### loading scraped schools
df_scrape = utils.load_scraped_data()
### loading mappings of shl and scraped schools
path_mapping = Path(__file__) / "../data/matching_shl_data_enriched.json"
with open(path_mapping, encoding="utf8") as json_file:
    data = json.load(json_file)
df_shl_enriched = pd.json_normalize(data)

# joining full scraped data on shl data with best matching scraped id and score
df = pd.merge(df_shl_enriched, df_scrape, on=['id_scraped'], how='left')
df['merge_status'] = None # initializing column for describing what is done with school (updated, untouched, new)

log.info('Overwriting shl data with scraped data in case of good matches')
# if threshold for score is reached shl information are overwritten with scraped data
for index, row in df.iterrows():
    if row['score'] >= 80:
        df.loc[index, 'name'] = df.loc[index, 'name_scraped']
        df.loc[index, 'anschrift'] = df.loc[index, 'anschrift_scraped']
        df.loc[index, 'anschrift2'] = df.loc[index, 'address2_scraped']
        df.loc[index, 'plz'] = df.loc[index, 'plz_scraped']
        df.loc[index, 'ort'] = df.loc[index, 'ort_scraped']
        df.loc[index, 'email'] = df.loc[index, 'email_scraped']
        df.loc[index, 'homepage'] = df.loc[index, 'website_scraped']
        df.loc[index, 'telefon'] = df.loc[index, 'phone_scraped']
        df.loc[index, 'telefax'] = df.loc[index, 'fax_scraped']
        df.loc[index, 'schultyp.id'] = None # information not computable for scraped data
        df.loc[index, 'schultyp.typ'] = df.loc[index, 'school_type_scraped']
        df.loc[index, 'deaktiviert'] = None # seems to be the value for active schools
        df.loc[index, 'merge_status'] = 'updated' # flag for shl schools which are updated with scraped data
    else:
        df.loc[index, 'id_scraped'] = None # matching score is too low, so connection to best match is removed
        df.loc[index, 'merge_status'] = 'untouched' # flag for shl schools which are not updated due to low score of best match

# keep only relevant columns of dataframe
df = df[['id', 'id_scraped', 'name', 'anschrift', 'anschrift2', 'plz', 'ort', 'email', 'homepage', 'telefon', 'telefax'
         , 'schultyp.id', 'schultyp.typ', 'deaktiviert', 'merge_status']]

# identify scraped schools which are not mapped to a shl school
df_new = df_scrape[~df_scrape['id_scraped'].isin(df['id_scraped'])]

log.info('Appending scraped schools which were not matched as new schools')
for index, row in df_new.iterrows():
    new_school = [None, df_new.loc[index, 'id_scraped'], df_new.loc[index, 'name_scraped'], df_new.loc[index, 'anschrift_scraped']
                 , df_new.loc[index, 'address2_scraped'] , df_new.loc[index, 'plz_scraped'], df_new.loc[index, 'ort_scraped']
                 , df_new.loc[index, 'email_scraped'], df_new.loc[index, 'website_scraped'], df_new.loc[index, 'phone_scraped']
                 , df_new.loc[index, 'fax_scraped'], None, df_new.loc[index, 'school_type_scraped'], None
                 , 'new'] # 'new' = flag for schools which have been scraped but were not matched to a shl school
    new_school_series = pd.Series(new_school, index=df.columns)
    df = df.append(new_school_series, ignore_index=True)

print(df.merge_status.unique)

log.info('Dumping data')

path_out = Path(__file__) / "../data/merging_shl_data_enriched.json"
df.to_json(path_out, orient='records')

endtime = datetime.datetime.now()

# calculating some stats
runtime = endtime - starttime

untouched = 0
for index, row in df.iterrows():
    if row['id'] != None and row['id_scraped'] == None:
    	untouched += 1
new = len(df_new.index)
total = len(df.index)
updated = total - new - untouched

updated_perc   = updated/total
untouched_perc = untouched/total
new_perc       = new/total

stats = [
['Runtime', runtime, ''],
['totalSchools', total, ''],
['updatedSchools', updated, updated_perc],
['untouchedSchools', untouched, untouched_perc],
['newSchools', new, new_perc],
]

df_stats = pd.DataFrame(stats)
path_out = Path(__file__) / "../data/merging_stats.csv"
df_stats.to_csv(path_out, index=False, sep=';', header=False)

log.info('Done')