"""Script for splitting the 8 large xml files of Läsning för barn so that each story is in a separate file. The script also creates a csv file mapping legacy id:s with the newly created file paths.
Created by Anna Movall and Jonas Lillqvist in March 2020"""

import os
from pathlib import Path
import re
from bs4 import BeautifulSoup

XML_OUTPUT_FOLDER = "Lfb_split_files/"

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
def create_directories(directory_name_base):
    for i in range(1,9):
        dir_name = XML_OUTPUT_FOLDER + directory_name_base + str(i)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

# creates a path object for a folder from a given filepath string, saves all paths to files found in this folder or subfolders in a list
def create_file_list(filepath):
    path = Path(filepath)
    filelist = []
    iterate_through_folders(path, filelist)
    return filelist

# iterates through folders recursively and appends filepaths to list
def iterate_through_folders(path, filelist):
    for content in path.iterdir():
        if content.is_dir():
            iterate_through_folders(content, filelist)
        elif content.suffix == ".xml":
            filelist.append(content)

# save content of each large file in a dictionary with its part nr as key
def read_file_content_to_dict(large_file_list):
    part_content_dict = {}
    i = 1
    for path in large_file_list:
        with path.open(encoding="utf-8") as source_file:
            content = source_file.read()
            part_content_dict[i] = content
            i += 1
    return part_content_dict

# create a file for each story in the right folder, using the story's name as basis for file name (transform it suitably)
# create file content using template xml and insert the right div from source files
# and title from lfb_list
def create_files(lfb_list, directory_name_base, part_content_dict):
    # one file is created for each item in the list
    for row in lfb_list:
        name = row[0]
        whole_id = row[1]
        part_nr = whole_id[0]
        div_id = whole_id[1:]
        # remove special characters from publication names and add suffix .xml
        file_name = create_file_name(name)
        new_file_path = directory_name_base + part_nr + "/" + file_name
        working_folder_path = XML_OUTPUT_FOLDER + new_file_path
        # get the right div from the right source file
        div_content = get_xml_content(part_nr, div_id, part_content_dict)
        # create file content using template xml, div_content and title from list
        with open(working_folder_path, "w", encoding="utf-8") as output_file:
            template_soup = content_template()
            # find the element where content is to be inserted
            template_div = template_soup.find(type="collection")
            # insert content
            template_div.append(div_content)
            # insert publication name as title
            template_title = template_soup.find("title")
            template_title.append(name)
            # write to file as string
            output_file.write(str(template_soup))
        # update list with the newly created file path
        row = add_db_file_path_to_list(row, new_file_path)
    return lfb_list

def add_db_file_path_to_list(row, new_file_path):
    db_file_path = "documents/trunk/Lasning_for_barn/" + new_file_path
    row.append(db_file_path)
    return row

def content_template():
    xml_template = '''
    <TEI xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://www.tei-c.org/ns/1.0" xsi:schemaLocation="http://www.tei-c.org/ns/1.0 file:/T:/Instruktioner,%20manualer,%20scheman/TEI-scheman%20(AM)/tei_barnschema.xsd">
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
    <div type="collection">

    </div>
    </body>
    </text>
    </TEI>
    '''
    return BeautifulSoup(xml_template, "xml")

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
    name = name + ".xml"
    return name

 # finds and returns the right div from the right source file
def get_xml_content(part_nr, div_id, part_content_dict):
    source_file_content = part_content_dict[int(part_nr)]
    soup = BeautifulSoup(source_file_content, "xml")
    div_content = soup.find(id=div_id)
    return div_content

# save parts of the updated list for later use
# only legacy id and file path are needed
# the file is needed for update_publication_with_filepaths.py
def write_list_to_csv(lfb_list, filename):
    with open(filename, "w", encoding="utf-8") as output_file:
        for row in lfb_list:
            csv_row = row[3] + ";" + row[4] + "\n"
            output_file.write(csv_row)

def main():
    # the starting point is a list of all the publications for which files need to be created
    lfb_list = create_list_from_csv("csv/Lfb_split.csv")
    # the files are created in folders whose name consist of this string and the part nr
    directory_name_base = "Lasning_for_barn_"
    create_directories(directory_name_base)
    large_file_list = create_file_list("Lasning_for_barn") # give path to folder with source files to be split
    part_content_dict = read_file_content_to_dict(large_file_list)
    lfb_list = create_files(lfb_list, directory_name_base, part_content_dict)
    write_list_to_csv(lfb_list, "csv/Lfb_signum_filer.csv")

main()