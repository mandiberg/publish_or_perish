import os
import pandas as pd
import numpy as np
import urllib.request
import sys
from urllib.error import HTTPError
import bibtexparser
import json
import pandas as pd
from habanero import Crossref
from thefuzz import fuzz, process
import csv
import time
import re

# Load CSV data
input_file = 'cleaned_data.csv'
cv_output_path = 'cv_entries.rtf'
JSON_FILE = "bibtex_entries.json"
NO_DOI_FILE = "no_doi.csv"
CROSSWALK_FILE = "crosswalk.csv"
EXCLUDE_PROJECTS = ["turbulece.org", "wikipedia", "eyebeam"]
SECTIONS = [
    "Historical Inclusion",
    "Case Studies",
    "Project and Exhibition Reviews",
    "Discussion",
    "Mentions"
]

TYPES_DICT = {
    "book chapter": "book chapter on",
    "case study": "case study of",
    "review": "review of",
    "discussion": "discussion of",
    "mention": "mention of"
}
VERBOSE = False

# Read CSV
df = pd.read_csv(input_file)
# def load_no_doi_file():
#     try:
#         df_no_doi = pd.read_csv(NO_DOI_FILE)
#     except:
#         df_no_doi = pd.DataFrame()
#         # create the file with headers
#         with open(NO_DOI_FILE, 'w', newline='') as no_doi_file:
#             writer = csv.writer(no_doi_file)
#             writer.writerow(["Authors", "Title", "Year", "Source"])
#     return df_no_doi

def load_csv(csv_file,header):
    try:
        df = pd.read_csv(csv_file)
        df.drop_duplicates()
    except:
        df = pd.DataFrame()
        # create the file with headers
        with open(csv_file, 'a', newline='') as csv_file:
            writer = csv.writer(csv_file,header)
            writer.writerow(header)
    return df

# df_no_doi = load_no_doi_file()
df_no_doi = load_csv(NO_DOI_FILE, ["Authors", "Title", "Year", "Source"])
df_crosswalk = load_csv(CROSSWALK_FILE, ["Authors", "Title", "Year", "ID"])

print(df_no_doi)
print(df_crosswalk)

def unicode_to_rtf(text):
    """Convert Unicode characters to RTF escape sequences.
    If a word is all capitals, convert it to title case before encoding."""
    if pd.isna(text):
        return ""
    
    # Split the text preserving punctuation and whitespace
    parts = re.split(r'(\W+)', str(text))
    rtf_text = ""
    list_of_words = ['LGBTQ','WP']
    for part in parts:
        # If the part is a word in all capitals (and longer than one letter), convert it
        if part.isupper() and len(part) > 1 and not any(word in part for word in list_of_words):
            part = part.title()
        for char in part:
            # Handle ASCII characters
            if ord(char) < 128:
                rtf_text += char
            # Handle Unicode characters
            else:
                rtf_text += f"\\u{ord(char)}?"
    return rtf_text

def float_to_int(value):
    """Convert float to int, handling NaN values."""
    if pd.isna(value):
        return None
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return value

def doi_to_bibtex(doi):
    base_url = "http://dx.doi.org/"
    url = base_url + doi
    req = urllib.request.Request(url)
    req.add_header('Accept', 'application/x-bibtex')
    try:
        with urllib.request.urlopen(req) as f:
            bibtex_str = f.read().decode('utf-8')
            # Format the BibTeX string to add line breaks after each field
            # print(bibtex_str)
            parser = bibtexparser.bparser.BibTexParser(common_strings=True)
            bib_database = bibtexparser.loads(bibtex_str, parser=parser)
            article_dict = bib_database.entries[0]
            # print(article_dict)
            return article_dict
            # formatted_bibtex = bibtexparser.dumps(bib_database)
            # return formatted_bibtex
    except HTTPError as e:
         print(f"HTTP Error: {e.code} for DOI: {doi}")
         return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def load_bibtex_entries():
    try:
        with open(JSON_FILE, 'r') as json_file:
            try:
                existing_entries = json.load(json_file)
            except json.JSONDecodeError:
                existing_entries = []
        # Convert existing entries to a DataFrame
    except FileNotFoundError:
        existing_entries = []

    df_existing = pd.DataFrame(existing_entries)
    df_existing = df_existing.drop_duplicates()
    return df_existing

def write_bibtex_entries(df):

    # Write the combined entries back to JSON_FILE
    with open(JSON_FILE, 'w') as json_file:
        json.dump(df.to_dict(orient='records'), json_file, indent=4)
    print(f"Saved {len(df)} BibTeX entries to {JSON_FILE}")

def get_bibtex_entries(df):
    df_bibtex = load_bibtex_entries()
    doi_list = df['DOI'].tolist()
    # drop na and 0 and " " values
    doi_list = [doi for doi in doi_list if pd.notna(doi) and doi != 0 and doi != " " and "/" in doi]
    print("doi_list", doi_list)
    # Get DOIs that are not already in the existing entries
    # save them all to json file
    existing_dois = df_bibtex['doi'].tolist() if not df_bibtex.empty else []
    print("existing dois", existing_dois)
    # Convert all DOIs to lowercase for comparison
    existing_dois_lower = [doi.lower() for doi in existing_dois if isinstance(doi, str)]
    new_dois = [doi for doi in doi_list if isinstance(doi, str) and doi.lower() not in existing_dois_lower]    
    # new_dois = [doi for doi in doi_list if doi not in existing_dois]
    print("new dois", new_dois)

    # Fetch BibTeX entries for new DOIs
    new_entries = []
    for doi in new_dois:
        bibtex_dict = doi_to_bibtex(doi)
        if bibtex_dict:
            new_entries.append(bibtex_dict)

    # Convert new entries to a DataFrame
    df_new = pd.DataFrame(new_entries)

    # Append new entries to the existing DataFrame
    df_combined = pd.concat([df_bibtex, df_new], ignore_index=True)

    write_bibtex_entries(df_combined)
    return df_combined

def flatten_crossref_data(data):
    def get_type(flattened):
        if 'type' in data:
            if not pd.isna(flattened["issn"]) or not pd.isna(flattened["issue"]) or not pd.isna(flattened["volume"]):
                flattened["ENTRYTYPE"] = "article"
            elif not pd.isna(flattened["isbn"]):
                # some kind of book
                flattened["ENTRYTYPE"] = "book_unspecified"
            # # elif not pd.isna(flattened["booktitle"]) or not pd.isna(flattened["isbn"]) or not pd.isna(flattened["collection"]) or not pd.isna(flattened["series"]):
            # elif data['type'] == 'book':
            #     flattened["ENTRYTYPE"] = "book"
            # elif data['type'] == 'book-chapter':
            #     flattened["ENTRYTYPE"] = "inbook"
            # else:
            #     flattened["ENTRYTYPE"] = data['type']
        else:
            flattened["ENTRYTYPE"] = "unknown"
        return flattened

    # print("Flattening data", data)
    """
    Flatten Crossref API response data into a one-dimensional dictionary
    similar to bibliography entries.
    
    Args:
        data (dict): The JSON data returned by Crossref API
    
    Returns:
        dict: A flattened dictionary with bibliographic information
    """
    flattened = {}
    
    # Extract author information
    if 'author' in data:
        authors = []
        for author in data['author']:
            if 'given' in author and 'family' in author:
                authors.append(f"{author['given']} {author['family']}")
        flattened["author"] = format_authors("; ".join(authors))
    
    # Extract basic metadata
    if 'title' in data and data['title']:
        flattened["title"] = data['title'][0]
    
    if 'container-title' in data and data['container-title']:
        flattened["journal"] = data['container-title'][0]
    else:
        flattened["journal"] = float('nan')  # Using NaN for missing values
    
    if 'publisher' in data:
        flattened["publisher"] = data['publisher']
    
    # Extract DOI and URL
    if 'DOI' in data:
        flattened["doi"] = data['DOI']
        flattened["url"] = f"http://dx.doi.org/{data['DOI']}"
    
    # Extract publication info
    if 'volume' in data:
        flattened["volume"] = data['volume']
    else:
        flattened["volume"] = float('nan')
    
    if 'issue' in data:
        flattened["number"] = data['issue']
    else:
        flattened["number"] = float('nan')
    
    # Extract publication date
    if 'published' in data and 'date-parts' in data['published']:
        date_parts = data['published']['date-parts'][0]
        if len(date_parts) >= 1:
            flattened["year"] = str(date_parts[0])
        if len(date_parts) >= 2:
            # Convert month number to name
            months = [
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ]
            month_num = date_parts[1]
            if 1 <= month_num <= 12:
                flattened["month"] = months[month_num - 1]
    
    # Extract ISSN
    if 'ISSN' in data and data['ISSN']:
        flattened["issn"] = data['ISSN'][0]
    else:
        flattened["issn"] = float('nan')
    
    # Add extra fields that appear in your examples
    flattened["ID"] = f"{data.get('author', [{}])[0].get('family', '')}_{flattened.get('year', '')}"
    flattened["booktitle"] = data.get('booktitle', float('nan'))
    flattened["isbn"] = data.get('isbn', float('nan'))
    flattened["collection"] = data.get('collection', float('nan'))
    flattened["series"] = data.get('series', float('nan'))
    flattened["pages"] = data.get('pages', float('nan'))  # No page information in the original data

    flattened["ENTRYTYPE"] = "article"

    return flattened

def fuzzy_test(value,search_column_value):
    if type(value) != type(search_column_value): return False
    if pd.isna(value) or pd.isna(search_column_value): return False
    if isinstance(value, float) or isinstance(search_column_value, float): return False
    string_length = len(value)
    # print("search_column_value", search_column_value)
    search_column_value = search_column_value[:string_length]
    matched_token=fuzz.token_set_ratio(value,search_column_value)
    # print("matched_token", matched_token, value, search_column_value)
    if matched_token> 80:
        return True
    return False

def find_index(df, value, search_column):
    matched_index = None
    if pd.isna(value) or isinstance(value, float): return None
    for columns in df.index:
        search_column_value = df.at[columns, search_column]
        if pd.isna(search_column_value): continue
        if fuzzy_test(value,search_column_value):
            matched_index = columns
            print("  find index: Matched index", matched_index)
        # string_length = len(value)
        # search_column_value = search_column_value[:string_length]
        # matched_token=fuzz.partial_ratio(value,search_column_value)
        # print("matched_token", matched_token, value, search_column_value)
        # if matched_token> 80:
        #     matched_index = columns
        #     print("Matched index", matched_index)
        #     # matched_vendors.append([vendor_name,regulated_vendor_name,matched_token])
    return matched_index

def search_crossref(df):
    df_bibtex = load_bibtex_entries()
    cr = Crossref()
    df = df.head(560)
    if not df_no_doi.empty: no_doi_titles = df_no_doi['Title'].to_list()
    else: no_doi_titles = []
    for index, row in df.iterrows():
        matched_index = None
        flattened_data = None
        doi = row['DOI']
        no_doi = None
        # print("Processing row", row)
        # print("Processing rowTitle", row['Title'])
        # check to see if we have already failed to look up this DOI
        if row['Title'] in no_doi_titles:
            print("- Already failed to find DOI for", row['Title'])
            continue
        elif not df_crosswalk.empty and row['Title'] in df_crosswalk['Title'].to_list():
            crosswalk_index = find_index(df_crosswalk, row['Title'], "Title")
            if crosswalk_index:
                # print("crosswalk index", crosswalk_index)
                cross_year = df_crosswalk.loc[crosswalk_index]["Year"]
                try:
                    if int(float(row['Year'])) == int(float(cross_year)):
                        print("+ Already found CROSSWALK for", row['Title'])
                        flattened_data = df_bibtex.loc[df_bibtex['ID'] == df_crosswalk.loc[crosswalk_index]["ID"]].to_dict(orient='records')[0]
                        # print("== flattened_data from crosswalk", flattened_data)
                except ValueError:
                    print(f"Skipping invalid year comparison for {row['Title']}: {row['Year']} vs {cross_year}")

        else: 
            # new search OR crosswalk needed
            if pd.notna(doi) and doi != 0 and "/" in doi:
                # if there is a doi
                if doi not in df_bibtex['doi'].tolist():
                    # and it isn't in the bibtex entries, search for it
                    print("Searching for DOI:", doi)
                    try:
                        result = cr.works(doi)
                    except:
                        print("Error in searching crossref for DOI", doi)
                        continue
                    if result['status'] == 'ok':
                        data = result['message']
                        flattened_data = flatten_crossref_data(data)
                        print("searched crossref via DOI and found", flattened_data)
                        if flattened_data['doi'] not in df_bibtex['doi'].to_list():
                            # Append the new entry to the existing DataFrame
                            df_bibtex = df_bibtex.append(flattened_data, ignore_index=True)
                        else:
                            print("+ DOI already in bibtex entries, not adding", row['Title'])
                else:
                    pass
                    print("+ DOI already in bibtex entries", row['Title'])
                    flattened_data = df_bibtex.loc[df_bibtex['doi'] == doi].to_dict(orient='records')[0]
                    print("flattened_data from bibtex", flattened_data)
            else:
                # no DOI
                # look for the entry the bibtex entries by row['Authors']} {row['Title']} {row['Year']} {row['Source']
                if not pd.isna(row['Title']):
                    print("Searching existing bibtex for query:", row['Authors'], row['Title'], row['Year'], row['Source'])
                    matched_index = find_index(df_bibtex, row['Title'], "title")
                    if VERBOSE: print("Matched index TITLE", matched_index)
                if not matched_index and not pd.isna(row['Source']):
                    matched_index = find_index(df_bibtex, row['Source'], "title")
                    if VERBOSE: print("Matched index SOURCE", matched_index)
                if matched_index is None:
                    if not pd.isna(row['Authors']):
                        matched_index = find_index(df_bibtex, row['Authors'], "author")
                        if VERBOSE: print("Matched index AUTHOR", matched_index)
                    if not matched_index:
                        matched_index = find_index(df_bibtex, row['Source'], "journal")
                        if matched_index:
                            # TK this is wrong, it needs to send in based on index
                            # and it needs to be replicated for each of these where index is matched
                            # maybe below at line 400?
                            matched_index = find_index(df_bibtex, row['Date'], "year")
                            if VERBOSE: print("Matched SOURCE YEAR already crossrefed", matched_index, df_bibtex.loc[matched_index]["title"])
                        else:
                            if VERBOSE: print("No SOURCE YEAR match found for query:", row['Authors'], row['Title'], row['Year'], row['Source'])
                    print("after everything, match_index is", matched_index)

                if matched_index is not None:
                    print("~ Matched index TITLE already crossrefed", matched_index, df_bibtex.loc[matched_index]["title"])
                    flattened_data = df_bibtex.loc[matched_index].to_dict()
                else:
                    def test_year_author(row, flattened_data):
                        authors = row.get('Authors', None)
                        year = row.get('Year', None)
                        flattened_author = flattened_data.get('author', None)
                        flattened_year = flattened_data.get('year', None)
                        is_match = False
                        if not pd.isna(authors) and authors == flattened_author:
                            # remove the first initial or two initials from author
                            authors_list = authors.split(" ")
                            for i, author in enumerate(authors_list):
                                if len(author) <= 2:
                                    authors_list.pop(i)
                            authors = " ".join(authors_list)
                            print("Matched AUTHOR", authors, flattened_author)
                            is_match = True
                        if is_match is False and not pd.isna(year) and year == flattened_year:
                            print("Matched YEAR", year, flattened_year)
                            is_match = True
                        return is_match
                    
                    # NO DOI
                    is_match = False
                    query = f"{row['Authors']} {row['Title']} {row['Year']} {row['Source']}"
                    print("Searching for query:", query)
                    try:
                        time.sleep(5)
                        result = cr.works(query=query)
                        if result['status'] == 'ok':
                            for index, data in enumerate(result['message']['items']):
                                # search all items for a match
                                data = result['message']['items'][index]
                                flattened_data = flatten_crossref_data(data)
                                print("flattened_data from crossref search item", index, flattened_data)
                                # test to see if the title OR source matches the resul
                                if not pd.isna(flattened_data.get('title', None)):
                                    if fuzzy_test(row['Title'],flattened_data['title']):
                                        # print("Matched title", row['Title'], data['title'][index])
                                        # check to see if the date matches
                                        is_match = test_year_author(row, flattened_data)
                                    elif fuzzy_test(row['Source'],flattened_data['title']):
                                        # print("Matched SOURCE", row['Source'], data['title'][index])
                                        is_match = test_year_author(row, flattened_data)
                                elif test_year_author(row, flattened_data):
                                    print("NO TITLE but Matched test_year_author", row, flattened_data)

                                if is_match: break
                                elif index == len(result['message']['items']) - 2 or index > 10: break
                                # else: print("No match found for query:", query, flattened_data.get('title', None))

                            if not is_match:
                                # save to NO_DOI_FILE
                                no_doi = [row['Authors'], row['Title'], row['Year'], row['Source']]
                                with open(NO_DOI_FILE, 'a', newline='') as no_doi_file:
                                    print("-- Writing to no_doi_file", no_doi)
                                    writer = csv.writer(no_doi_file)
                                    writer.writerow(no_doi)
                    except RuntimeError as e:
                        # sleep for 5 seconds
                        time.sleep(25)
                        print(f"RuntimeError: {e} for query: {query}")
                        continue

                    if is_match:
                        # check to see if there is a DOI, and if so, get that bibtex to use bc it is better
                        print("Searched and found:",flattened_data)
                        if 'DOI' in data:
                            flattened_data = doi_to_bibtex(flattened_data['doi'])
                            print("bibtex from DOI", flattened_data)
                        # else:
                        #     flattened_data = flatten_crossref_data(data)
                        # Append the new entry to the existing DataFrame
                        df_bibtex = df_bibtex.append(flattened_data, ignore_index=True)
        if flattened_data is not None and no_doi is None:
            print("going to add flattened_data", flattened_data)
            print("to this row of df", df.loc[index])
            if not crosswalk_index:
                crosswalk = [row['Authors'], row['Title'], row['Year'], flattened_data.get("ID", None)]
                with open(CROSSWALK_FILE, 'a', newline='') as crosswalk_file:
                    print("Writing to crosswalk", crosswalk)
                    writer = csv.writer(crosswalk_file)
                    writer.writerow(crosswalk)
            # add flattened_data to df index row
            for key, value in flattened_data.items():
                bib_key = f"bib_{key}"
                df.loc[index, bib_key] = value

                # key = "bib_"+key
                # df.loc[index, key] = value
            # df.loc[index, "bib_Authors"] = flattened_data.get("author", None)
            # df.loc[index, "bib_Title"] = flattened_data.get("title", None)
            # df.loc[index, "bib_booktitle"] = flattened_data.get("booktitle", None)
            # df.loc[index, "bib_journal"] = flattened_data.get("journal", None)
            # df.loc[index, "bib_volume"] = flattened_data.get("volume", None)
            # df.loc[index, "bib_number"] = flattened_data.get("number", None)
            print("ADDED this row of df", df.loc[index])
        else:
            print(" >< >< No flattened_data for this row of df", df.loc[index]['Title'])



            # add the bibtext info to existing df. Or add df index to bibtex for later merge?
            # add unique ID to df index. Non mutable.         "ID": "Wexelbaum_2019",
            # df.loc[index, "ID"] = flattened_data["ID"]
            # f"{flattened_data['author']}_{flattened_data['year']}"
            # print("Matched index", matched_index)
            # Append the new entry to the existing DataFrame

    # Write the combined entries back to JSON_FILE
    write_bibtex_entries(df_bibtex)

    # # join the two dataframes on the ID
    # df = df.set_index('ID')
    # df_bibtex = df_bibtex.set_index('ID')
    # df = df.join(df_bibtex, how='left', on='ID')
    print("df", df)

    return df

    # query = "D Robb	Exploring Imbalances on Wikipedia Through Archival Creation Theories	2024	The iJournal: Student Journal of the Faculty of"
    # results = cr.works(query=query)
    # print(results['message']['items'][0])

    # flattened_data = flatten_crossref_data(results['message']['items'][0])
    # print(flattened_data)


def make_project_string(row):
    # print(row['Project'])
    if pd.isna(row['Project']) or row['Project']==0 or row['Project']=="0":
        project_str = "_TK_"
        print(row['Title'], "Project is NaN", row['Project'])
        return project_str
    row['Project'] = list(set(row['Project'].split(", ")))
    # drop values "turbulece.org" and "wikipedia" from row['Project'] list
    row['Project'] = [project for project in row['Project'] if project not in EXCLUDE_PROJECTS]
    
    term_count = len(row['Project'])
    project_str = " "
    for i, this_term in enumerate(row['Project']):
        if not pd.isna(this_term):
            if term_count > 1 and i == term_count -1:
                project_str += "and "
            project_str += this_term
            if i < term_count - 1:
                project_str += ", "
    return project_str

def format_authors(authors):
    print("parsing authors", authors)
    """
    Format author names from various input styles to 'Firstname Lastname' format.
    
    Args:
        authors (str): Authors as a string, with names separated by commas or semicolons
    
    Returns:
        str: Formatted author names in 'Firstname Lastname' format
    """
    def parse_name(name):
        print("parsing name", name)
        if pd.isna(name) or name == "" or name == " ":
            return name
        # Remove extra whitespace
        name = name.strip()
        
        # Check if name contains a comma (last name, first name format)
        if ',' in name:
            print("Comma in name", name)
            parts = name.split(',')
            # if len(parts[1]) == 2 and parts[1].strip().isupper():
            #     parts[1] = parts[1][0]
            return f"{parts[1].strip()} {parts[0].strip()}"
        
        # Standard first name last name format
        parts = name.split()
        if len(parts[0]) == 2 and parts[0].strip().isupper():
            parts[0] = parts[0][1:]

        return name if len(parts) <= 2 else f"{' '.join(parts[:-1])} {parts[-1]}"

    # Split authors by semicolon or comma, and parse each
    if ";" in authors:
        author_list = [author.strip() for author in authors.split(';')]
    elif "and" in authors:
        author_list = [author.strip() for author in authors.split(' and ')]
    elif "," in authors:
        author_list = [author.strip() for author in authors.split(',')]
        if len(author_list) == 2 and len(author_list[0].split()) == 1 and len(author_list[1].split()) == 1:
            author_list = [authors]
        # else:
        #     author_list = [author_list[0]] + [" and ".join(author_list[1:])]
    else:
        author_list = [authors]
    # author_list = [author.strip() for author in authors.replace(';', ',').split(',')]
    # print("author_list", author_list)
    
    # Parse and format each author name
    formatted_authors = [parse_name(author) for author in author_list if author]
    # print("formatted_authors", formatted_authors)
    
    return ', '.join(formatted_authors)

def format_volume_issue(row):
    # format volume, issue
    print("format_volume_issue full row", row)
    volume = row['pq_volume'] if not pd.isna(row.get('pq_volume')) else row['bib_volume'] if not pd.isna(row.get('bib_volume')) else row['Volume']
    issue = row['pq_issue'] if not pd.isna(row.get('pq_issue')) else row['bib_number'] if not pd.isna(row.get('bib_number')) else row['Issue']

    volume = float_to_int(volume)
    issue = float_to_int(issue)

    if volume is None and issue is None:
        return None
    elif volume is None or volume == 0:
        return f"({issue})"
    elif issue is None or issue == 0:
        return f"{volume}"
    else:
        return f"{volume}({issue})"

def make_cv_entry(row, project_str, show_year=True):
    # print(row['Title'], row['Year'], row['ReviewType'],  project_str)
    if pd.notna(row['Year']):
        year = int(row['Year'])
    else:
        year = 1000
    
    # determine if book, or edited volume
    is_book = False
    is_edited_volume = False

    if VERBOSE: print(row['Title'], "type", row['Type'])

    # I need to refactor this to handle the different types better


    type_dict = {
        "edited volume": "inbook",
        "Ed. Book": "inbook",
        "book chapter": "inbook",
        "book": "book",
        "article": "article",
        "BA Thesis": "Thesis",
        "Masters Thesis": "Thesis",
        "Disseration": "Disseration"
    }
    
    pub_type = row.get('ENTRYTYPE') if pd.notna(row.get('ENTRYTYPE')) else type_dict[row.get('culled_COMMENTS')] if row.get('culled_COMMENTS') in type_dict else type_dict[row.get('Type')] if row.get('Type') in type_dict else None

    # if not pd.isna(row['Type']) and type(row['Type']) != float:
    #     if row['Type'].lower() == "book" or not pd.isna(row['Chapter']):
    #         is_book = True
    #     elif row.get('ENTRYTYPE', None) == 'inbook' or row['Type'].lower() == "edited volume" or not pd.isna(row['Chapter']) or not pd.isna(row['Chapter_Auth']):
    #         is_edited_volume = True
    authors = row.get('bib_author') if pd.notna(row.get('bib_author')) else row.get('pq_Authors') if pd.notna(row.get('pq_Authors')) else row.get('Authors') if pd.notna(row.get('Authors')) else None
    title = row.get('bib_title') if pd.notna(row.get('bib_title')) else row.get('pq_Title') if pd.notna(row.get('pq_Title')) else row.get('Title') if pd.notna(row.get('Title')) else None
    source = row.get('bib_journal') if pd.notna(row.get('bib_journal')) else row.get('bib_booktitle') if pd.notna(row.get('bib_booktitle')) else row.get('pq_pubtitle') if pd.notna(row.get('pq_pubtitle')) else row.get('Source') if pd.notna(row.get('Source')) else None
    
    # # Convert all text fields to RTF Unicode escape sequences
    # if not pd.isna(row['pq_Title']) and not row['pq_Title'].isspace():
    #     # using proquest data
    #     authors_col = 'pq_Authors'
    #     title_col = 'pq_Title'
    #     source_col = 'pq_pubtitle'
    # elif not pd.isna(row['bib_title']) and not row['bib_title'].isspace():
    #     # using crossref data
    #     authors_col = 'bib_author'
    #     title_col = 'bib_title'
    #     if not pd.isna(row['bib_journal']):
    #         source_col = 'bib_journal'
    #     elif not pd.isna(row['bib_booktitle']):
    #         source_col = 'bib_booktitle'
    # else:
    #     authors_col = 'Authors'
    #     title_col = 'Title'
    #     source_col = 'Source'

    authors = unicode_to_rtf(authors)
    authors = format_authors(authors)
    title = unicode_to_rtf(title)
    source = unicode_to_rtf(source)

    Chapter_Auth = unicode_to_rtf(row['Chapter_Auth'])
    Chapter = unicode_to_rtf(row['Chapter'])

    volume_issue = format_volume_issue(row)
    # Start building the entry
    if show_year: cv_entry = f"{year}\\tab "
    else: cv_entry = "\\tab "

    # format entry based on type
    if volume_issue is not None or pub_type == "article":
        cv_entry += f"{authors}, \"{title},\" \\i {source}\\i0\~{volume_issue}"
    elif pub_type == "book":
        cv_entry += f"{authors}, \\i {title}, \\i0 {source}"
    elif pub_type == "inbook":
        cv_entry += f"{Chapter_Auth}, \"{Chapter}\", in \\i {title}\\i0, {authors} ed, {source}"
    else:
        cv_entry += f"{authors}, \"{title},\" \\i {source}\\i0"
    
    # Add page numbers
    pages = None
    if (isinstance(row['pq_pages'], str) and row['pq_pages'].isspace()) or row['pq_pages'] == 0 or (isinstance(row['StartPage'], str) and row['StartPage'].isspace()) or row['StartPage'] == 0:
        pass # pages stay None
    elif pd.notna(row['pq_pages']):
        pages = row['pq_pages']
    elif pd.notna(row['StartPage']):
        pages = row['StartPage']
        if pd.notna(row['EndPage']):
            pages = row['StartPage'] + "-" + row['EndPage']
    if VERBOSE: print(f"pages: {pages}")
    if pages and "-" in pages:
        cv_entry += f", pages {pages}"
    elif pages:
        cv_entry += f", page {pages}"
    
    # print(row['Title'], row['Volume'], row['Issue'], row['ReviewType'])

    if pd.notna(row['Volume']) and not row['Volume'].isspace():
        # is Volume a space character?
        if VERBOSE: print("[make_cv_entry] Volume is not NaN", row['Volume'])
        cv_entry += f", Volume {row['Volume']}"
    if pd.notna(row['Issue']):
        cv_entry += f", Issue {row['Issue']}"
    if row['ReviewType']:
        if VERBOSE: print("[make_cv_entry] ", row['Title'], row['ReviewType'])
        if row['ReviewType']==0 or row['ReviewType']=="0":
            print(row['Title'], "ReviewType is 0", row['ReviewType'])
            row['ReviewType']="TK"
        type_key = unicode_to_rtf(row['ReviewType'])
        # print(type_key)
        type_str = TYPES_DICT.get(type_key, type_key)
        cv_entry += f", {type_str}"
        # print(type_str)
    else:
        cv_entry += f", mention of"
    if project_str:
        project_str = unicode_to_rtf(project_str)
        # remove leading space
        if project_str[0] == " ":
            project_str = project_str[1:]
        cv_entry += f" {project_str}."
    return cv_entry.replace(", ,", ",").replace("  "," ")

def sort_entries(entries_df):
    """Sort entries by year in reverse chronological order, putting NaN years at the end"""
    # Convert Year to float to handle NaN values
    entries_df['Year'] = pd.to_numeric(entries_df['Year'], errors='coerce')
    # Sort by Year descending, putting NaN at the bottom
    return entries_df.sort_values('Year', ascending=False, na_position='last')

# Add section header formatting
def format_section_header(section_name):
    return f"\\par\\par\\b {section_name}\\b0\\par"

df_bibtex = get_bibtex_entries(df)
print("df_bibtex", df_bibtex)

df_crossref = search_crossref(df)
print("df_bibtex", df_crossref)

df = df_crossref # assign the crossref data to main df

# Process each section
all_entries = []

for section in SECTIONS:
    section_entries = []
    
    # Filter rows based on section criteria
    if section == "Historical Inclusion":
        section_df = df[df['special'] == "historical"].copy()
    elif section == "Case Studies":
        section_df = df[(df['ReviewType'] == "case study") & (df['special'] != "historical")].copy()
    elif section == "Project and Exhibition Reviews":
        section_df = df[(df['ReviewType'] == "review") & (df['special'] != "historical")].copy()
    elif section == "Discussion":
        section_df = df[(df['ReviewType'] == "discussion") & (df['special'] != "historical")].copy()
    else:  # Mentions
        section_df = df[(df['ReviewType'] == "mention") & (df['special'] != "historical")].copy()
    print(section, len(section_df))
    # Sort the section entries
    if not section_df.empty:
        section_df = sort_entries(section_df)
        
        # Group entries by year
        current_year = None
        
        # Generate entries for this section
        for _, row in section_df.iterrows():
            print("about to make entry for", row)
            project_str = make_project_string(row)
            year = row['Year'] if pd.notna(row['Year']) else None
            
            # Determine if we should show the year
            show_year = year != current_year
            
            # Generate the entry
            cv_entry = make_cv_entry(row, project_str, show_year)
            section_entries.append(cv_entry)
            print("entry", cv_entry)
            # Update current year
            current_year = year
        
        # Add section header and entries if section is not empty
        if section_entries:
            all_entries.append(format_section_header(section))
            all_entries.extend(section_entries)

# Save CV entries to an RTF file with UTF-8 encoding declaration
with open(cv_output_path, 'w', encoding='utf-8') as cv_file:
    # RTF header with UTF-8 encoding
    cv_file.write("{\\rtf1\\ansi\\ansicpg65001\\deff0\n")
    # Font table declaration
    cv_file.write("{\\fonttbl{\\f0\\froman\\fcharset0 Times New Roman;}}\n")
    # Set default font
    cv_file.write("\\f0\\fs24\n")
    # cv_file.write("\\par\n")
    cv_file.write('\n\\par '.join(all_entries))
    cv_file.write("\n}")

print(f"CV entries saved to {cv_output_path}")