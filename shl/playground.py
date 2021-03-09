import utils
import json
import os
from pathlib import Path
import pandas as pd
import datetime

path_merging = Path(__file__) / "../data/merging_shl_data_enriched.json"
with open(path_merging, encoding="utf8") as json_file:
    data = json.load(json_file)
df_shl_enriched = pd.json_normalize(data)

def find_multimatches(df_shl_enriched):
    ### FIND SCRAPED SCHOOLS WHICH WERE MAPPED ON MORE THAN ONE SHL SCHOOL IN MERGED SHL DATA
    id_scraped_occurences = df_shl_enriched['id_scraped'].value_counts()
    id_scraped_multimatches = id_scraped_occurences[id_scraped_occurences > 1].keys()
    # get shl schools where matched scraped school is not unique
    df_shl_enriched_multimatches = df_shl_enriched[df_shl_enriched['id_scraped'].isin(id_scraped_multimatches)]

    # load full data of matching
    path_matching = Path(__file__) / "../data/matching_full_data.csv"
    df_matching_full_data = pd.read_csv(path_matching, sep=';')

    # filter full data of matching on shl schools where matched scraped school is not unique
    df_matching_multimatches = df_matching_full_data[df_matching_full_data['id'].isin(df_shl_enriched_multimatches['id'])].sort_values(by=['id_scraped'])
    # it's better to analyze multimatches via full data of matching rather than via enriched shl data as comparison of shl value and scraped value is possible and score is visible
    path_out = Path(__file__) / "../data/matching_multimatches.csv"
    df_matching_multimatches.to_csv(path_out, index=False, sep=';', header=True)


def validate_matches(df_shl_enriched):
    ### VALIDATE MATCHED DATA WITH LABELLED SCHOOLS FROM CIMT AG
    # load validation data from cimt ag
    # Zur Erklärung der Daten
    # "session_id" – ID der (Browser-)Session
    # "timestamp"- Zeitpunkt der manuellen Validierung als UNIX-Timestamp
    # "id_shl" – ID der gematchen Schule in der SHl-DB
    # "id_jedeschule" – ID der gematchten Schule bei jedeschule
    # "is_same" – Ergebnis der Validierung (-1 ungleich, 0 unsicher, 1 gleiche Schule)
    # "comment" – Freitextfeld
    path_validate = Path(__file__) / "../data/in/validation_202103031203.csv"
    df_validate = pd.read_csv(path_validate, sep=',')

    # inner join merged data and validation data
    df_joined = pd.merge(df_shl_enriched, df_validate, left_on=['id'], right_on=['id_shl'], how='inner')

    df_joined['result'] = None
    true_positives = true_negatives = false_positives = false_negatives = matches_unsure = 0
    for index, row in df_joined.iterrows():
        if row['is_same'] == 1 and row['id_scraped'] == row['id_jedeschule']:
            df_joined.at[index, 'result'] = 'true_positive'
            true_positives += 1 # shl school was CORRECTLY matched
        elif row['is_same'] == 1 and row['id_scraped'] != row['id_jedeschule']:
            df_joined.at[index, 'result'] = 'false_negative'
            false_negatives += 1 # shl school was WRONGLY matched to no scraped school or another scraped school
        elif row['is_same'] == -1 and row['id_scraped'] == row['id_jedeschule']:
            df_joined.at[index, 'result'] = 'false_positive'
            false_positives += 1 # shl school was WRONGLY matched to the scraped school
        elif row['is_same'] == -1 and row['id_scraped'] != row['id_jedeschule']:
            df_joined.at[index, 'result'] = 'true_negative'
            true_negatives += 1 # shl school was CORRECTLY not matched to the scraped school
        elif row['is_same'] == 0:
            df_joined.at[index, 'result'] = 'unsure'
            matches_unsure += 1
        else:
            print('Something went wrong with id_shl ' + row['id_shl'])

    total_validate_schools = len(df_joined)
    validate_schools = total_validate_schools - matches_unsure
    print('Total schools for validation: ' + str(total_validate_schools))
    print(str(validate_schools) + ' schools were used for validation as ' + str(matches_unsure) + ' were labelled -unsure- by validators')
    print('CORRECT matches or CORRECT no matches:\t\t' + str(true_positives+true_negatives) 
    	  + ' (' + str(round((true_positives+true_negatives)/validate_schools*100, 1)) + ' %)')
    print('INCORRECT matches or INCORRECT no matches:\t' + str(false_positives+false_negatives)
    	  + ' (' + str(round((false_positives+false_negatives)/validate_schools*100, 1)) + ' %)')
    df_joined_out = df_joined[['id_shl', 'id_jedeschule', 'is_same', 'id_scraped', 'result']]
    path_out = Path(__file__) / "../data/validation.csv"
    df_joined_out.to_csv(path_out, index=False, sep=';')


### choose what has to be done:
# find_multimatches(df_shl_enriched)
# validate_matches(df_shl_enriched)