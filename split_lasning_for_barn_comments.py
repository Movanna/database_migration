"""
Script for splitting the general comments for collection Lasning for barn.
Comments were delivered as one Word file, then transformed to one XML file.
The script creates an XML file for each comment, adds the right content and saves the file path.
Comment info is then inserted into the database and connected to the right publication in the db.  
Created by Anna Movall and Jonas Lillqvist in March/April 2020.
"""

import os
from pathlib import Path
import re
import psycopg2
from bs4 import BeautifulSoup

conn_new_db = psycopg2.connect(
    host="",
    database="",
    user="",
    port="",
    password=""
)
cursor_new = conn_new_db.cursor()

XML_SOURCE_FILE = ""
DIRECTORY_NAME_BASE = "Lasning_for_barn_"
CSV_LIST = "csv/Lfb_split.csv"

# creates a list from csv file with publication name, div id, publication id and legacy id
def create_list_from_csv(filename):
    with open(filename, "r", encoding="utf-8") as source_file:
        lfb_list = []
        for line in source_file:
            row = line.rstrip()
            elements = row.split(";")
            lfb_list.append(elements)
        return lfb_list

# creates a folder for each of the 8 parts
def create_directories(DIRECTORY_NAME_BASE):
    for i in range(1,9):
        dir_name = DIRECTORY_NAME_BASE + str(i) + "_komm"
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

def read_text_from_file(source_file_path):
    with source_file_path.open(encoding="utf-8") as source_file:
        content = source_file.read()
    return content

# save each main div, containing comments to one part, in a dictionary with the part nr as key
# the divs are saved as Beautiful Soup objects
def create_part_dict(comment_xml):
    comment_soup = BeautifulSoup(comment_xml, "xml")
    part_content_dict = {}
    i = 1
    for element in comment_soup.body.children:
        if element.name == "div":
            part_content_dict[i] = element
            i += 1
    return part_content_dict

# create a file for each comment in the right folder, using the corresponding publication's name as basis for file name (transform it suitably)
# create file content using template xml and insert content from the right div in dictionary
# insert title from lfb_list
def create_files(lfb_list, DIRECTORY_NAME_BASE, part_content_dict):
    # one file is created for each item in the list
    for row in lfb_list:
        name = row[0]
        whole_id = row[1]
        part_nr = whole_id[0]
        # remove special characters from publication names and add suffix .xml
        file_name = create_file_name(name)
        new_file_path = DIRECTORY_NAME_BASE + part_nr + "_komm" + "/" + file_name
        # get the right div as a soup object from the right source file
        div_content = get_xml_content(part_nr, name, part_content_dict)
        # remove head element from div_content 
        div_content.head.decompose()
        # extract bibliography for later use
        bibliography = div_content.find(rend="Litteratur")
        if bibliography is not None:
            bibliography.extract()
        # create file content using template xml, div_content and title from list
        with open(new_file_path, "w", encoding="utf-8") as output_file:
            template_soup = content_template()
            # find the element where content is to be inserted
            template_comment_div = template_soup.find(type="comment")
            # insert comment div contents without its own div
            template_comment_div.append(div_content)
            template_comment_div.div.unwrap()
            # insert publication name as title
            template_title = template_soup.find("title")
            template_title.append(name)
            # insert bibliography
            if bibliography is not None:
                template_bibl_div = template_soup.find(type="bibl")
                template_bibl_div.append(bibliography)
            # write to file as string
            output_file.write(str(template_soup))
        # update list with the newly created file path
        row = add_db_file_path_to_list(row, new_file_path)
    return lfb_list

# adds xml file path to one row in list of comment data
# it will later be inserted in the db
def add_db_file_path_to_list(row, new_file_path):
    db_file_path = "documents/Redaktionella_texter/Kommentarer/Lasning_for_barn/" + new_file_path
    row.append(db_file_path)
    return row

def content_template():
    xml_template = '''
    <TEI xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.sls.fi/tei file:/T:/Instruktioner,%20manualer,%20scheman/TEI-scheman%20(AM)/tei_redtextschema.xsd" xmlns="http://www.tei-c.org/ns/1.0">
    <teiHeader>
    <fileDesc>
      <titleStmt>
        <title></title>
        <respStmt>
          <resp/>
          <name/>
        </respStmt>
      </titleStmt>
      <publicationStmt>
        <publisher>Zacharias Topelius Skrifter</publisher>
      </publicationStmt>
      <sourceDesc>
        <p/>
      </sourceDesc>
    </fileDesc>
    </teiHeader>
    <text>
    <body xml:space="preserve">
    <div type="comment">
    <lb/>

    </div>
    <div type="notes">
    </div>
    <div type="bibl">
    </div>
    </body>
    </text>
    </TEI>
    '''
    return BeautifulSoup(xml_template, "xml")

# creates comment file name using publication name as starting point
def create_file_name(name):
    # remove special characters from publication names
    name = re.sub(r",|\.|\?|!|–|’|»|:|(|)|\[|\]|&", "", name).strip()
    name = name.replace(" ", "_").lower()
    name = name.replace("-", "_")
    name = name.replace("ä", "a")
    name = name.replace("å", "a")
    name = name.replace("ö", "o")
    name = name.replace("é", "e")
    name = name.replace("ü", "u")
    name = name.replace("æ", "ae")
    # add file suffix
    name = name + "_komm.xml"
    return name

 # finds and returns the right comment div from dictionary
 # the head element in the comment div contains the commented publication's name
 # it should match the name of the publication from the list
def get_xml_content(part_nr, name, part_content_dict):
    part_div = part_content_dict[int(part_nr)]
    comments = part_div.select("div > div")
    comment_div = None
    for comment in comments:
        main_title = comment.head.get_text()
        if main_title.lower() == name.lower():
            comment_div = comment
            break
    return comment_div

# writes parts of the updated list to file for later use
# only legacy id and file path are needed
def write_list_to_csv(lfb_list, filename):
    with open(filename, "w", encoding="utf-8") as output_file:
        for row in lfb_list:
            csv_row = row[3] + ";" + row[4] + "\n"
            output_file.write(csv_row)

# in order to update the db we need the new publication id
def get_id_from_publication(legacy_id):
    fetch_query = """SELECT id FROM publication WHERE legacy_id = %s"""
    cursor_new.execute(fetch_query, (legacy_id,))
    publication_id = cursor_new.fetchone()
    return publication_id

# insert comment data into table publication_comment
# then update table publication with the comment id
def create_comment_data(lfb_list):
    for row in lfb_list:
        legacy_id = row[3]
        filepath = row[4]
        published = 1 # published internally
        publication_id = get_id_from_publication(legacy_id)
        insert_query = """INSERT INTO publication_comment(published, legacy_id, original_filename) VALUES (%s, %s, %s) RETURNING id"""
        values_to_insert = (published, legacy_id, filepath)
        cursor_new.execute(insert_query, values_to_insert)
        # get newly created comment id
        comment_id = cursor_new.fetchone()[0]
        # update table publication with the comment id for this publication
        update_query = """UPDATE publication SET publication_comment_id = %s WHERE id = %s"""
        values_to_insert = (comment_id, publication_id)
        cursor_new.execute(update_query, values_to_insert)
    conn_new_db.commit()
    conn_new_db.close()
    cursor_new.close()

def main():
    # the starting point is a list of all the publications for which comment files need to be created
    lfb_list = create_list_from_csv(CSV_LIST)
    # the files are created in folders whose name consist of this string and the part nr
    create_directories(DIRECTORY_NAME_BASE)
    source_file_path = Path(XML_SOURCE_FILE)
    comment_xml = read_text_from_file(source_file_path)
    part_content_dict = create_part_dict(comment_xml)
    lfb_list = create_files(lfb_list, DIRECTORY_NAME_BASE, part_content_dict)
    write_list_to_csv(lfb_list, "csv/Lfb_kommentarer_filer.csv")
    create_comment_data(lfb_list)

main()