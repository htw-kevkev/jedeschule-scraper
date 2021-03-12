import csv
import json
import uuid
import datetime
import asyncio
import cx_Oracle as cxO
import pandas as pd
from elasticsearch import Elasticsearch, AsyncElasticsearch
from matching_results import Results

es_address = [
    {
        'host': 'localhost',
        'port': 9200
    }
]

INDEX_NAME = "school"

aes = AsyncElasticsearch(es_address)
es = Elasticsearch(es_address)
cache = Results("")


def get_db_content():
    cursor = cxO.connect('u570443', 'p570443', cxO.makedsn('oradbs02.f4.htw-berlin.de', '1521', sid='oradb1'), encoding='UTF-8').cursor()
    cursor.execute("SELECT * FROM schools")

    return cursor


def create_index():
    start_time = datetime.datetime.now()
    data = read_json()

    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "schultyp": {
                        "type": "object"
                    }
                }
            }
        }
    )

    bulk_data = []
    for index, row in data.iterrows():

        data_dict = {}
        for i in range(len(row)):
            data_dict[data.columns[i]] = row[i]

        op_dict = {
            'index': {
                "_index": INDEX_NAME,
                "_id": uuid.uuid4()
            }
        }

        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

    es.bulk(index=INDEX_NAME, body=bulk_data)

    es.indices.refresh(index=INDEX_NAME)
    print(es.cat.count(index=INDEX_NAME))
    print("Index created and filled in: " + str(datetime.datetime.now() - start_time))


def delete_all_docs():
    start_time = datetime.datetime.now()

    es.delete_by_query(index=INDEX_NAME, body={
        "query": {
            "match_all": {}
        }
    })

    print("All documents deleted in: " + str(datetime.datetime.now() - start_time))

    es.indices.delete(index=INDEX_NAME)


def read_json():
    with open('localdev/20191203-020430-schools-ohne-ansprechpartner.json', encoding='utf8') as json_file:
        data = json.load(json_file)
    return pd.json_normalize(data).fillna("")


async def match():
    cursor = get_db_content()

    counter = 0
    while True:
        row = cursor.fetchone()

        if row is None:
            cursor.close()
            break

        row = [str(i or '') for i in row]

        name = row[0]
        address = row[2]
        zip = row[4]

        res = await aes.search(
            index=INDEX_NAME,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "plz": zip
                                }
                            }
                        ],
                        "should": [
                            {
                                "match": {
                                    "name": {
                                        "query": name,
                                        "fuzziness": "AUTO"
                                    }
                                }
                            },
                            {
                                "match": {
                                    "anschrift": {
                                        "query": address,
                                        "fuzziness": "AUTO"
                                    }
                                }
                            }
                        ]
                    }
                }
            },
            size=1,
        )

        counter += 1

        # A score of 16 was determined to be most likely the end of the grey zone
        es_score = res["hits"]["max_score"]
        if es_score is not None and es_score > 16:
            found_school = res["hits"]["hits"][0]
            source = found_school["_source"]
            school_type = ""

            # Due to ES, object-like data structures get flattened.
            # This results in two possible cases for the schultyp-field
            if "schultyp.typ" in source:
                school_type = source["schultyp.typ"]
            elif "schultyp" in source:
                school_type = source["schultyp"]["typ"]

            await aes.update(
                index=INDEX_NAME,
                id=found_school["_id"],
                body={
                    "doc": {
                        "id": row[1] if row[1] != "" else source["id"],
                        "name": row[0] if row[0] != "" else source["name"],
                        "anschrift": row[2] if row[2] != "" else source["anschrift"],
                        "anschrift2": row[3] if row[3] != "" else source["anschrift2"],
                        "plz": row[4] if row[4] != "" else source["plz"],
                        "ort": row[5] if row[5] != "" else source["ort"],
                        "homepage": row[6] if row[6] != "" else source["homepage"],
                        "email": row[7] if row[7] != "" else source["email"],
                        "schultyp": {
                            "typ": school_type
                        },
                        "telefax": row[11] if row[11] != "" else source["telefax"],
                        "telefon": row[12] if row[12] != "" else source["telefon"],
                        "MERGE_STATUS": "updated"
                    }
                }
            )

        else:
            await aes.index(
                index=INDEX_NAME,
                id=uuid.uuid4(),
                body={
                    "id": row[1],
                    "name": row[0],
                    "anschrift": row[2],
                    "anschrift2": row[3],
                    "plz": row[4],
                    "ort": row[5],
                    "homepage": row[6],
                    "email": row[7],
                    "schultyp": {
                        "typ": row[8]
                    },
                    "telefax": row[11],
                    "telefon": row[12],
                    "MERGE_STATUS": "new"
                }
            )

        print("Matching Nr." + str(counter))


def dump_index_to_json():
    scroll_data = es.search(
        index=INDEX_NAME,
        scroll="2m",
        size=1000,
        body={
            "query": {
                "match_all": {}
            }
        }
    )

    scroll_id = scroll_data["_scroll_id"]
    scroll_size = len(scroll_data["hits"]["hits"])

    dump = []
    while scroll_size > 0:

        for school in scroll_data["hits"]["hits"]:
            school_source = school["_source"]

            if "MERGE_STATUS" not in school_source:
                school_source["MERGE_STATUS"] = "untouched"

            dump.append(school_source)

        scroll_data = es.scroll(
            scroll_id=scroll_id,
            scroll="2m"
        )
        scroll_id = scroll_data["_scroll_id"]
        scroll_size = len(scroll_data["hits"]["hits"])
        print(str(len(dump)))

    with open("merged.json", "w", encoding='utf8') as outfile:
        json.dump(dump, outfile, indent=4, ensure_ascii=False)


def start_matching():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(match())


def main():
    start_time = datetime.datetime.now()

    start_matching()
    cache.print_results(matches=False, no_matches=False, ranges=False, merged=False)

    print("Data matched in: " + str(datetime.datetime.now() - start_time))


def compare_with_validation():
    cursor = cxO.connect('u570443', 'p570443', cxO.makedsn('oradbs02.f4.htw-berlin.de', '1521', sid='oradb1'),
                         encoding='UTF-8').cursor()

    with open('localdev/validation_202103031203.csv', newline='') as csv_file:
        spamreader = csv.reader(csv_file, delimiter=',', quotechar='|')

        counter = 0
        for row in spamreader:
            if counter > 0:
                cursor.execute("SELECT * FROM schools WHERE ID = :js_id", js_id=row[3].replace('"', ""))
                table_row = cursor.fetchone()

                res = es.search(
                    index=INDEX_NAME,
                    body={
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "plz": table_row[4]
                                        }
                                    }
                                ],
                                "should": [
                                    {
                                        "match": {
                                            "name": {
                                                "query": table_row[0],
                                                "fuzziness": "AUTO"
                                            }
                                        }
                                    },
                                    {
                                        "match": {
                                            "anschrift": {
                                                "query": table_row[2],
                                                "fuzziness": "AUTO"
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                )["hits"]["hits"]

                if len(res) > 0:
                    print(str(counter + 1) + " " + str({
                        "score": res[0]["_score"],
                        "scraped": {
                            "id": row[3],
                            "plz": table_row[4],
                            "name": table_row[0],
                            "adress": table_row[2]
                        },
                        "shl": {
                            "id": res[0]["_source"]["id"],
                            "plz": res[0]["_source"]["plz"],
                            "name": res[0]["_source"]["name"],
                            "adress": res[0]["_source"]["anschrift"]
                        }
                    }))
                else:
                    print("No match: " + row[3])

            counter += 1

        cursor.close()


compare_with_validation()
'''
delete_all_docs()
create_index()

main()

dump_index_to_json()
'''