import json
from pathlib import Path


def write_anything(file_name, data):
    with open(file_name, "w", encoding='utf8') as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False)


class Results:

    directory = ""

    not_matched = []
    matched = []
    merged = []
    ranges_by_ten = {
        "0-14": [],
        "14-16": [],
        "16-100": []
    }

    def __init__(self, directory):
        self.directory = directory

    def no_matches(self, row):
        self.not_matched.append({
            "name": row[0],
            "address": row[2],
            "zip": row[4]
        })

    def matches(self, row, res):
        hits = res["hits"]
        max_score = hits["max_score"]
        matches_dict = {
            "db": {
                "name": row[0],
                "address": row[2],
                "zip": row[4]
            },
            "es": hits["hits"],
            "max_score": max_score
        }

        if 0 < max_score <= 14:
            self.ranges_by_ten["0-14"].append(matches_dict)
        elif 14 < max_score <= 16:
            self.ranges_by_ten["14-16"].append(matches_dict)
        elif 16 < max_score <= 100:
            self.ranges_by_ten["16-100"].append(matches_dict)

        self.matched.append(matches_dict)

    def add_to_merged(self, row):
        self.merged.append({
            "id": row[1],
            "name": row[0],
            "anschrift": row[2],
            "anschrift2": row[3],
            "plz": row[4],
            "ort": row[5],
            "homepage": row[6],
            "email": row[7],
            "schultyp": row[8],
            "telefax": row[11],
            "telefon": row[12],
            "MERGE_STATUS": "new"
        })

    def add_update_to_merged(self, row, res):
        found_school = res["hits"]["hits"][0]["_source"]
        self.merged.append({
            "id":  row[1] if row[1] != "" else found_school["id"],
            "name": row[0] if row[0] != "" else found_school["name"],
            "anschrift": row[2] if row[2] != "" else found_school["anschrift"],
            "anschrift2": row[3] if row[3] != "" else found_school["anschrift2"],
            "plz": row[4] if row[4] != "" else found_school["plz"],
            "ort": row[5] if row[5] != "" else found_school["ort"],
            "homepage": row[6] if row[6] != "" else found_school["homepage"],
            "email": row[7] if row[7] != "" else found_school["email"],
            "schultyp": row[8] if row[8] != "" else found_school["schultyp.typ"],
            "telefax": row[11] if row[11] != "" else found_school["telefax"],
            "telefon": row[12] if row[12] != "" else found_school["telefon"],
            "MERGE_STATUS": "updated"
        })

    def print_results(self, matches, no_matches, ranges, merged):
        if merged:
            write_anything(self.directory + "merged.json", self.merged)

        if matches:
            write_anything(self.directory + "matched.json", self.matched)

        if no_matches:
            write_anything(self.directory + "not_matched.json", self.not_matched)

        if ranges:
            for key in self.ranges_by_ten.keys():
                dict_arr = self.ranges_by_ten[key]

                if len(dict_arr) > 0:
                    Path(self.directory + "ranges").mkdir(parents=True, exist_ok=True)
                    write_anything(self.directory + "ranges/r" + key + ".json", dict_arr)

