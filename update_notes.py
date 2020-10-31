"""
Script that updates the document_id:s in table documentnote in db topelius_notes.
Läsning för barn consisted of 8 XML files; they were split into about 300, and
the document id:s for them needed to be changed in the database containing lemmas and comments.
Created by Anna Movall and Jonas Lillqvist in May 2020.
"""

import mysql.connector
from pathlib import Path
from bs4 import BeautifulSoup

conn_old_db = mysql.connector.connect(
    host="",
    database="",
    port="",
    user="",
    passwd="",
    charset="utf8"
)
cursor_old = conn_old_db.cursor()

OLD_DOCUMENT_ID = (4395, 4396, 4397, 4398, 4399, 4400, 4401, 4402)
XML_SOURCE_FOLDER = "Lfb_split_files"

# from table documentnote, fetch the id for each lemma belonging to the old Lfb-files
def get_lemma_id():
    fetch_query = """SELECT id FROM documentnote WHERE document_id IN (%s, %s, %s, %s, %s, %s, %s, %s)"""
    values = OLD_DOCUMENT_ID
    cursor_old.execute(fetch_query, values)
    lemma_id = cursor_old.fetchall()
    return lemma_id

# create path object for folder from given filepath string, save all paths to files found in this folder or subfolders in a list
def create_file_list():
    path = Path(XML_SOURCE_FOLDER)
    filelist = []
    iterate_through_folders(path, filelist)
    return filelist

# iterate through folders recursively and append filepaths to list
def iterate_through_folders(path, filelist):
    for content in path.iterdir():
        if content.is_dir():
            iterate_through_folders(content, filelist)
        else:
            filelist.append(content)

# find out which file a lemma belongs to and return the file path for the file
def find_lemma_in_file(lemma_id, xml_filepath_list):
    xml_id_value = "start" + str(lemma_id)
    for filepath in xml_filepath_list:
        with filepath.open(encoding="utf-8") as xml_file:
            soup = BeautifulSoup(xml_file, "xml")
            anchor = soup.find(attrs={"xml:id" : xml_id_value})
            if anchor:
                return(filepath)
    return False

# get document_id from db using file path
def fetch_document_id(filepath):
    fetch_query = """SELECT id FROM document WHERE path = %s"""
    value = (filepath,)
    cursor_old.execute(fetch_query, value)
    document_id = cursor_old.fetchone()[0]
    if document_id:
        return document_id
    print(filepath, "not found")
    return False

# update table documentnote with the new document_id for each lemma
def update_document_id(new_document_id, lemma_id):
    update_query = """UPDATE documentnote SET document_id = %s WHERE id = %s"""
    values_to_insert = (new_document_id, lemma_id)
    cursor_old.execute(update_query, values_to_insert)

def main():
    lemma_ids = get_lemma_id()
    xml_filepath_list = create_file_list()
    for lemma_id in lemma_ids:
        filepath = find_lemma_in_file(lemma_id[0], xml_filepath_list)
        if filepath:
            folder = filepath.parts[1]
            filename = filepath.parts[2]
            filepath = "/documents/trunk/Lasning_for_barn/" + folder + "/" + filename
            new_document_id = fetch_document_id(filepath)
            if new_document_id:
                update_document_id(new_document_id, lemma_id[0])
                print(filepath, new_document_id)
    conn_old_db.commit()
    cursor_old.close()
    conn_old_db.close()

main()