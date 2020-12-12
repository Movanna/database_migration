# database_migration
Python scripts for migrating a MySQL database to a PostgreSQL database with a different structure.

The databases belong to an edition project, Zacharias Topelius Skrifter, which publishes the works of the author Zacharias Topelius. The edition is published on a website (http://topelius.sls.fi), as e-books and in print. The PostgreSQL database is part of a generic digital edition platform, managed by the Society of Swedish Literature in Finland (see https://github.com/slsfi/digital_edition_documentation/wiki).

The database contains information about editions, texts, manuscripts, versions, facsimiles and editorial texts. The scripts listed below were written in the first half of 2020 and then used for migrating the relevant information. The new database improves the work flow of the publishing process considerably by storing more information, such as file paths for each text. Before, when (re)publishing a text on the web, the editor had to choose the file path by hand, and it wasn't permanently stored. On the new platform, this is no longer necessary. A lot of the functionality in the scripts deals with finding out the correct file paths for each text. Obviously, we didn't want to add 7 000 file paths manually into the new database.

There is also a script which creates the tables of contents for the editions (TOC was a table in the old database, but each TOC is a separate JSON file on the new platform). Another script splits large XML files into smaller ones, based on information from the database. Yet another one uses an API to fetch metadata for URL:s and inserts it into the database. The two last scripts update a remaining MySQL database, connected to the new platform.

All scripts were written in a pair programming context.

# Database migration scripts for Zacharias Topelius Skrifter

As a general rule, the scripts only migrate data concerning collections that are or will be a part of the published edition.

Data belonging to previously planned but abandoned collections is not migrated.

The scripts should be run in the following order: 

## 1. migrate_main_tables.py
Enter the project name as the value of the variable project_name before running the script.

The script creates two JSON files which map id:s from the old db to id:s created in the new db, in the id_dictionaries folder.

## 2. migrate_manuscripts_and_versions.py
This script uses publication_ids.json which was created by script 1.

The script creates two JSON files which map id:s from the old db to id:s created in the new db, in the id_dictionaries folder.

## 3. migrate_facsimiles.py
This script uses publication_ids.json which was created by script 1 and manuscript_ids.json which was created by script 2.

The script creates a JSON file which map id:s from the old db to id:s created in the new db, in the id_dictionaries folder.

It creates a log file with info about facsimiles which do not belong to migrated publications. In some cases, these facsimiles appear elsewere on the website and might need special attention.

## 4. create_comment_data.py
This script uses collection_ids.json which was created by script 1.

Old collection id:s and relative paths to their comment folders is given as a list of tuples to variable old_collections.

It creates a log file containing the publications and the comment file paths which matched them. It also creates a log file containing publications for which no comment file path was found.

## 5. update_publication_with_filepaths.py
This script uses collection_ids.json and publication_ids.json which were created by script 1.

It imports four functions from script 4, create_comment_data.py.

Old collection id:s and relative paths to their reading text folders is given as a list of tuples to variable old_collections.

Publication names and their matching reading text file names for collections Publicistik, Forelasningar and Lasning for barn are stored in three separate documents: ZTS_Publicistik_verk_signum_filer.csv, Forelasningar_signum_filer.csv and Lfb_signum_filer.csv.

It creates a log file containing the publications and the reading text file paths which matched them. It also creates a log file containing publications for which no reading text file path was found.

## 6. update_manuscript_with_filepaths.py
This script uses collection_ids.json which was created by script 1.

Old collection id:s and relative paths to their manuscript folders is given as a list of tuples to variable old_collections.

It creates a log file containing the publications and the manuscript file paths which matched them and a log file containing publications for which no manuscript file path was found. It also creates a log file containing manuscripts files with the same title; these file paths need to be inserted into the database manually.

## 7. update_version_with_filepaths.py
This script uses collection_ids.json which was created by script 1.

Old collection id:s and relative paths to their version folders is given as a list of tuples to variable old_collections.

The script uses copies of all the version XML files from the web server; they need to be stored in a subfolder named var.

It creates five log files: one containing the publications and the version file paths which matched them; one containing versions for which no file path was found; one containing publications and version file paths from the matching directory/directories; one containing publication versions for which no matching directory was found; one containing version file paths which were matched several times (they need to be checked manually).

## 8. create_toc.py
Enter the project id as the value of the variable PROJECT_ID before running the script.

This script uses collection_ids.json which was created by script 1 and Lfb_split.csv (for Läsning för barn).

The script fetches info from table tableofcontents in old db and transforms it into one properly ordered toc JSON file for each new collection. It sorts the toc items based on different values in the db.

## 9. split_lasning_for_barn.py
This script uses the XML files for Lasning for barn and the list Lfb_split.csv, which contains publication info.

The script splits the 8 large XML files so that each story/publication is in a separate file.

The script also creates a CSV file (Lfb_signum_filer.csv) mapping legacy id:s with the newly created file paths. This file is used by update_publication_with_filepaths.py.

## 10. split_lasning_for_barn_comments.py
This script uses Lfb_split.csv, which contains publication info, and Lfb_kommentarer.xml, which contains all the general comments for Lasning for barn.

The script creates an XML file for each comment, adds the right content and saves the file path. It then inserts comment info into the database, connecting it to the right publication.

The script also creates a CSV file (Lfb_kommentarer_filer.csv) mapping legacy id:s with the newly created file paths.

## 11. facsimile_url_info.py
This script uses publicistiktabell.csv to create a list of facsimile legacy id:s and url:s.

It fetches metadata based on URL using API and inserts it into table publication_facsimile_collection. The script also inserts id:s into table publication_facsimile.

## 12. create_introduction_and_title.py
This script uses collection_ids.json, which was created by script 1, and introduction_title_names.csv, which contains the name bases for each title page and introduction XML file. It also needs the current project_id.

It inserts data for introductions and title pages and updates table publication_collection with the corresponding ids.

## 13. update_notes.py
This script updates the document_id:s in table documentnote in topelius_notes for collection Läsning för barn. It has to be run only once; document_id:s are not continously updated. The XML files need to be accessible for the script.

## 14. update_comment_with_lasning_for_barn.py
This script was used for updating table document in db topelius_notes with filepaths to comments for Läsning för barn.
