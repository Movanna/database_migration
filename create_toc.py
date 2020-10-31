"""
Script that fetches info from table tableofcontents in old db and transforms it
into one toc json file for each new collection.
Created by Anna Movall and Jonas Lillqvist in March 2020.
"""

import mysql.connector
import psycopg2
import json
import operator
import re

# insert current project id here
PROJECT_ID = 10

conn_old_db = mysql.connector.connect(
    host="",
    database="",
    user="",
    passwd=""
)
cursor_old = conn_old_db.cursor()

conn_new_db = psycopg2.connect(
    host="",
    database="",
    user="",
    port="",
    password=""
)
cursor_new = conn_new_db.cursor()

def read_dict_from_file(filename):
    with open(filename, encoding="utf-8") as source_file:
        json_content = json.load(source_file)
        return json_content

# get info about toc items in one collection from old db 
def get_toc_info(old_collection_id):
    # collection Letters (Brev) was split into two new collections (30, 31), which are not part of old table publications or tableofcontents, only table publications_collections
    # we need to change their old collection id to the actual old id 15 and not use  publications_collections id 30/31, found in the coll_id_dict
    if old_collection_id == "30":
        fetch_query = """SELECT tableofcontents.title, toc_date, toc_linkID, tableofcontents.sortOrder, publications_group.sortOrder FROM tableofcontents, publications_group WHERE toc_zts_id=%s AND toc_coll_id=%s AND toc_group_id=group_id"""
        values_to_insert = (15, 1)
    elif old_collection_id == "31":
        fetch_query = """SELECT title, toc_date, toc_linkID, sortOrder FROM tableofcontents WHERE toc_zts_id=%s AND toc_coll_id=%s"""
        values_to_insert = (15, 2)
    else:
        fetch_query = """SELECT title, toc_date, toc_linkID, sortOrder FROM tableofcontents WHERE toc_zts_id=%s"""
        values_to_insert = (old_collection_id,)
    cursor_old.execute(fetch_query, values_to_insert)
    toc_info = cursor_old.fetchall()
    # the date value for Brev needs to be edited so that None is substituted with "0"
    # otherwise sorting by date is not possible
    # to be editable, the tuples in toc_info need to be lists
    if old_collection_id == "30" or old_collection_id == "31":
        toc_info_list = []
        for tuple in toc_info:
            row_list = list(tuple)
            if row_list[1] is None:
                row_list[1] = "0"
            toc_info_list.append(row_list)
    # for Forlagskorrespondens, sort based on publications_group.sortOrder, then based on date 
    if old_collection_id == "30":
        toc_info_sorted = sorted(toc_info_list, key = operator.itemgetter(4,1))
    # for Foraldrakorrespondens, sort based on date
    elif old_collection_id == "31":
        toc_info_sorted = sorted(toc_info_list, key = operator.itemgetter(1))
    # for other collections, sort based on sortOrder
    else:
        toc_info_sorted = sorted(toc_info, key = operator.itemgetter(3))
    return toc_info_sorted

# creates toc dictionary, used for json file
def create_dictionary(toc_info_sorted, old_collection_id, new_collection_id):
    # use this if the dictionary has collections with no publications in the db
    if len(toc_info_sorted) == 0:
        collection_toc_dict = {}
        print("List empty. Collection id old/new: ", old_collection_id, new_collection_id)
        return False
    # the first row in the list contains the name of the collection
    collection_name = toc_info_sorted[0][0]
    # create first level of dictionary as required for json toc
    collection_toc_dict = {"text": collection_name, "collectionId": str(new_collection_id), "type": "title", "children": []}
    # loop through toc_info_sorted, skip first row which contains collection name
    for i in range(1, len(toc_info_sorted)):
        row = toc_info_sorted[i]
        text_title = row[0]
        toc_date = row[1]
        toc_linkID = row[2]
        # skip rows which refer to letters listed in tableofcontents, with no publication linked to them; these have "Mibr" in toc_linkID 
        if toc_linkID is not None:
            match = re.search("Mibr", toc_linkID)
            if match is not None:
                continue
        # an item id is required for the json toc items
        itemId = add_itemId(row, old_collection_id, new_collection_id)
        # special rule for Forelasningar, which contains descriptions pertaining to a title
        # the text should be added to the previous toc item with key "description"; no new toc item is created
        if itemId == "" and old_collection_id == "20":
            toc_item_dict["description"] = text_title
            continue
        # toc items which stand for sections and do not link to texts have no itemId 
        elif itemId == "":
            toc_type = "section_title"
        # items with itemId are links to reading texts, with type est    
        else:
            toc_type = "est"
        if toc_date is None:
            toc_date = ""
        # create dict for toc item and append it to list of children of first level dict
        toc_item_dict = {"url": "", "type": toc_type, "text": text_title, "itemId": itemId, "date": toc_date}
        collection_toc_dict["children"].append(toc_item_dict)
    return collection_toc_dict

# constructs itemId based on new collection id and new publication id
# to find out publication id, we need legacy_id
# which is constructed using old collection id and toc_linkID from old db
def add_itemId(row, old_collection_id, new_collection_id):
    toc_linkID = row[2]
    if toc_linkID is None or toc_linkID == "":
        itemId = ""
    # a toc_linkID that contains ch + 1-3 digits and possibly pos + 1-3 digits is a special kind of link
    # it refers to a part of a file (a div or an anchor-element) 
    # the part beginning with ch or pos, the fragment_id, needs to be added to itemID
    # but it has to be removed from toc_linkID before constructing the legacy_id
    else:
        pattern = re.compile(r";(ch\d{1,3}(;pos\d{1,4})?)")
        match = re.search(pattern, toc_linkID)
        if match is not None:
            fragment_id = match.group(1)
            toc_linkID = re.sub(pattern, "", toc_linkID)
        else:
            fragment_id = 0
        # collection Letters (Brev) was split into two new collections (30, 31), which are not part of old table publications or tableofcontents, only table publications_collections
        # we need to change their old collection id to the actual old id 15 and not use  publications_collections id 30/31, found in the coll_id_dict
        if old_collection_id == "30" or old_collection_id == "31":
            old_collection_id = "15"
        legacy_id = old_collection_id + "_" + toc_linkID
        publication_id = fetch_publication_id(legacy_id)
        if fragment_id == 0:
            itemId = str(new_collection_id) + "_" + str(publication_id)
        else:
            itemId = str(new_collection_id) + "_" + str(publication_id) + "_" + fragment_id
    return itemId

# get publication_id from new db using legacy_id
# limit selection to publications connected to the current publication collection
def fetch_publication_id(legacy_id):
    fetch_query = """SELECT id FROM publication WHERE legacy_id=%s AND publication_collection_id IN (SELECT id FROM publication_collection WHERE project_id=%s)"""
    value_to_insert = (legacy_id, PROJECT_ID)
    cursor_new.execute(fetch_query, value_to_insert)
    result = cursor_new.fetchone()
    if result is None:
        print(legacy_id, "not found in publication")
        publication_id = ""
    else:
        publication_id = result[0]
    return publication_id

def write_dict_to_file(dictionary, filename):
    json_dict = json.dumps(dictionary, ensure_ascii=False)
    with open(filename, "w", encoding="utf-8") as output_file:
        output_file.write(json_dict)
        print("Dictionary written to file", filename)

# special function for generating toc for Lfb: values from csv, not from table tableofcontents    
def create_toc_for_Lfb(filename, collection_id_dict):
    lfb_list = create_list_from_csv(filename)
    new_collection_id = collection_id_dict["32"]
    collection_toc_dict = {"text": "Läsning för barn", "collectionId": str(new_collection_id), "type": "title", "children": []}
    for row in lfb_list:
        title = row[0]
        legacy_id = row[3]
        publication_id = fetch_publication_id(legacy_id)
        itemId = str(new_collection_id) + "_" + str(publication_id)
        toc_item_dict = {"url": "", "type": "est", "text": title, "itemId": itemId, "date": ""}
        collection_toc_dict["children"].append(toc_item_dict)
    filename = "toc_files/" + str(new_collection_id) + ".json"
    write_dict_to_file(collection_toc_dict, filename)
    
# creates a list from csv file with publication name, div id, publication id and legacy id
def create_list_from_csv(filename):
    with open(filename, "r", encoding="utf-8") as source_file:
        lfb_list = []
        for line in source_file:
            row = line.rstrip()
            elements = row.split(";")
            lfb_list.append(elements)
        return lfb_list

def main():
    collection_id_dict = read_dict_from_file("id_dictionaries/collection_ids.json")
    for key in collection_id_dict.keys():
        old_collection_id = key # string value!
        # toc for Lasning for barn is created from csv file with a special function
        if old_collection_id == "32":
            continue
        new_collection_id = collection_id_dict[old_collection_id]
        toc_info_sorted = get_toc_info(old_collection_id)
        # toc_info_sorted will be an empty list if key is not found in tableofcontents!
        collection_toc_dict = create_dictionary(toc_info_sorted, old_collection_id, new_collection_id)
        if collection_toc_dict:
            filename = "toc_files/" + str(new_collection_id) + ".json"
            write_dict_to_file(collection_toc_dict, filename)
    create_toc_for_Lfb("csv/Lfb_split.csv", collection_id_dict)
    conn_new_db.close()
    cursor_new.close()
    conn_old_db.close()
    cursor_old.close()

main()