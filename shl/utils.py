# -*- coding: utf-8 -*-
"""
Created on Sun Oct  4 11:05:19 2020

@author: kevin.kretzschmar
"""

import logging
import sys
import pandas as pd 
from pathlib import Path
import os
import json

loggers = {}

def set_logging(name="logger"):
    """
    To avoid repeating logging set-up
    :return: logger
    """
    global loggers

    if loggers.get(name):
        return loggers.get(name)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        loggers[name] = logger
        return logger

def string_cleaning(col):
    return col.replace(',',' ').replace(';', ' ').replace('\t',' ').replace('\n', ' ').replace('\r',' ')

def clean_data(df):
    # format "straße" for fuzzy matching to get a better score
    df['plz'] = df['plz'].apply('{:0>5}'.format)
    df['anschrift'] = df['anschrift'].str.replace('str. ','straße ')
    df['anschrift'] = df['anschrift'].str.replace('STR. ','STRAße ')
    df['anschrift'] = df['anschrift'].str.replace('Str. ','straße ')
    df['anschrift'] = df['anschrift'].str.replace('-straße ','-Straße ')
    df['anschrift'] = df['anschrift'].str.replace('"','')
    df = df.fillna('leer')
    return df 

def load_scraped_data():
    ### loading scraped schools from json files in directory 'jedeschule-scraper/data'
    all_data = []
    path_scrape = Path(__file__) / "../../data/"
    for file in os.listdir(path_scrape):
        if file.endswith(".json"):
            with open(os.path.join(path_scrape, file), encoding="utf8") as json_file:
                data = json.load(json_file)
                for d in data:
                    name = string_cleaning(d['info']['name'])
                    id = string_cleaning(d['info']['id'])
                    try:
                        address = string_cleaning(d['info']['address'])
                    except:
                        address = None

                    try:
                        address2 = string_cleaning(d['info']['address2'])
                    except:
                        address2 = None

                    try:
                        zip = string_cleaning(d['info']['zip'])
                    except:
                        zip = None

                    try:
                        city = string_cleaning(d['info']['city'])
                    except:
                        city = None

                    try:
                        website = string_cleaning(d['info']['website'])
                    except:
                        website = None

                    try:
                        email = string_cleaning(d['info']['email'])
                    except:
                        email = None

                    try:
                        school_type = string_cleaning(d['info']['school_type'])
                    except:
                        school_type = None

                    try:
                        legal_status = string_cleaning(d['info']['legal_status'])
                    except:
                        legal_status = None

                    try:
                        provider = string_cleaning(d['info']['provider'])
                    except:
                        provider = None
                    
                    try:
                        fax = string_cleaning(d['info']['fax'])
                    except:
                        fax = None

                    try:
                        phone = string_cleaning(d['info']['phone'])
                    except:
                        phone = None
                    
                    try:
                        director = string_cleaning(d['info']['director'])
                    except:
                        director = None

                    all_data.append([name, id, address, address2, zip, city, website, email, school_type, legal_status, provider, fax, phone, director])
        else:
            continue

    df = pd.DataFrame(all_data, columns=['name', 'id', 'address', 'address2', 'zip', 'city', 'website', 'email'
        , 'school_type', 'legal_status', 'provider', 'fax', 'phone', 'director'])
    # removing duplicates (Nebenstandorte!)
    df = df.drop_duplicates(subset=['name', 'zip'], keep='first')
    # removing duplicates again but on id now as we need it to be unique
    df = df.drop_duplicates(subset=['id'], keep='first')
    # renaming relevant columns along shl names
    df = df.rename(columns={'zip': 'plz', 'address': 'anschrift', 'city': 'ort'})
    df = clean_data(df)
    # adding suffixes to columns to avoid same column names in shl and scraped data
    df = df.add_suffix('_scraped')
    return df

def load_shl_data():
    ### loading shl schools
    path_shl = Path(__file__) / "../data/in/shl_data.json"
    with open(path_shl, encoding="utf8") as json_file:
        data = json.load(json_file)
    df = pd.json_normalize(data)
    df_clean = clean_data(df)
    return df, df_clean