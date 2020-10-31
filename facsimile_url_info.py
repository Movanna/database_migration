"""Script that fetches metadata based on url using API and inserts it into table publication_facsimile_collection. The script also inserts id:s into table publication_facsimile.
Created by Anna Movall and Jonas Lillqvist in March 2020.
Information about the API (see section see OAI-PMH):
https://wiki.helsinki.fi/display/Comhis/Interfaces+of+digi.kansalliskirjasto.fi"""

import psycopg2
import re
import requests
from bs4 import BeautifulSoup
from datetime import date

conn_new_db = psycopg2.connect(
    host="",
    database="",
    user="",
    port="",
    password=""
)
cursor_new = conn_new_db.cursor()

CSV_FILEPATH = ""

# creates a list from csv file containing old publication id and facsimile url
def create_list_from_csv(filename):
    with open(filename, "r", encoding="utf-8") as source_file:
        facsimile_url_list = []
        for line in source_file:
            row = line.rstrip()
            elements = row.split(";")
            legacy_id = elements[0]
            url = elements[1]
            publication_id = get_publication_id_from_legacy_id(legacy_id)
            if publication_id:
                facsimile_url_list.append([publication_id, url])
        return facsimile_url_list

# get new id from table publication using old id
def get_publication_id_from_legacy_id(legacy_id):
    fetch_query = """SELECT id FROM publication WHERE legacy_id=%s"""
    value_to_insert = (legacy_id,)
    cursor_new.execute(fetch_query, value_to_insert)
    result = cursor_new.fetchone()
    if result:
        publication_id = result[0]
    else:
        publication_id = False
    return publication_id

# fetches metadata for each facsimile using api
# the api request url contains the binding_id, which is part of the facsimile url
# the date and title metadata is then processed to an html string in the right format and appended to the (sub)list
def add_metadata_to_list(facsimile_url_list):
    for row in facsimile_url_list:
        url = row[1]
        binding_id = get_binding_id(url)
        api_url = "https://digi.kansalliskirjasto.fi/interfaces/OAI-PMH?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:digi.kansalliskirjasto.fi:" + binding_id
        r = requests.get(api_url)
        metadata_soup = BeautifulSoup(r.content, "xml")
        title = metadata_soup.find("dc:title").get_text()
        date_string = metadata_soup.find("dc:date").get_text()
        try:
            date_object = date.fromisoformat(date_string)
            year = date_object.year
            month = date_object.month
            day = date_object.day
            date_info = str(day) + "/" + str(month) + " " + str(year)
        #if the date has no month and day, use it as it is
        except ValueError:
            print("Invalid iso date.")
            date_info = date_string
        # split the title info into two elements at comma, to separate the journal title and nr info
        title_elements = title.split(",")
        if len(title_elements) == 2:
            pub_title = title_elements[0]
            pub_nr = title_elements[1].lstrip()
            pub_nr = pub_nr.replace("nr:", "nr")
            db_title = "<cite>" + pub_title + "</cite> " + date_info + ", " + pub_nr
            row.append(db_title)
            print(row)
        # if the title has no nr info, use it as it is
        else:
            db_title = "<cite>" + title + "</cite>, " + date_info
            row.append(db_title)
            print(row)
            print(url, "has irregular metadata: ", title)

# finds the binding_id component in facsimile url  
def get_binding_id(url):
    pattern = re.compile(r"/(\d{4,7})\?")
    match = re.search(pattern, url)
    binding_id = match.group(1)
    return binding_id

# the csv facsimile url list was in chronological order
# this order needs to be preserved in the db as the value of priority in publication_facsimile
# the function gives the first row with a given id priority 1, the second priority 2 etc.
def set_facsimile_order(facsimile_url_list):
    publication_id = 0
    for row in facsimile_url_list:
        if row[0] != publication_id:
            priority = 1
        else:
            priority += 1
        row.append(priority)
        publication_id = row[0]

# inserts needed values into table publication_facsimile_collection, returning the newly created id for the facsimile
# calls function create_publication_facsimile which inserts facsimile id and publication id into table publication_facsimile
def create_publication_facsimile_collection(facsimile_url_list):
    insert_query = """INSERT INTO publication_facsimile_collection(title, external_url) VALUES (%s, %s) RETURNING id"""
    for row in facsimile_url_list:
        publication_id = row[0]
        url = row[1]
        title = row[2]
        priority = row[3]
        values_to_insert = (title, url)
        cursor_new.execute(insert_query, values_to_insert)
        facsimile_id = cursor_new.fetchone()[0]
        create_publication_facsimile(publication_id, facsimile_id, priority)

# inserts facsimile id and publication id into table publication_facsimile
def create_publication_facsimile(publication_id, facsimile_id, priority):
    insert_query = """INSERT INTO publication_facsimile(publication_facsimile_collection_id, publication_id, page_nr, section_id, priority, type) VALUES (%s, %s, %s, %s, %s, %s)"""
    values_to_insert = (facsimile_id, publication_id, 0, 0, priority, 0)
    cursor_new.execute(insert_query, values_to_insert)

def main():
    facsimile_url_list = create_list_from_csv(CSV_FILEPATH)
    add_metadata_to_list(facsimile_url_list)
    set_facsimile_order(facsimile_url_list)
    create_publication_facsimile_collection(facsimile_url_list)
    conn_new_db.commit()
    cursor_new.close()
    conn_new_db.close()

main()