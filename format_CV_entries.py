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

# Load CSV data
input_file = 'cleaned_data.csv'
cv_output_path = 'cv_entries.rtf'
JSON_FILE = "bibtex_entries.json"
NO_DOI_FILE = "no_doi.csv"
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

def unicode_to_rtf(text):
    """Convert Unicode characters to RTF escape sequences."""
    if pd.isna(text):
        return ""
    
    rtf_text = ""
    for char in str(text):
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
    doi_list = [doi for doi in doi_list if pd.notna(doi) and doi != 0 and doi != " "]
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

    print("Flattening data", data)
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
    string_length = len(value)
    search_column_value = search_column_value[:string_length]
    matched_token=fuzz.token_set_ratio(value,search_column_value)
    # print("matched_token", matched_token, value, search_column_value)
    if matched_token> 80:
        return True
    return False

def find_index(df, value, search_column):
    matched_index = None
    for columns in df.index:
        search_column_value = df.at[columns, search_column]
        if fuzzy_test(value,search_column_value):
            matched_index = columns
            # print("Matched index", matched_index)
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
    df = df.head(10)
    for _, row in df.iterrows():
        doi = row['DOI']
        if pd.notna(doi) and doi != 0:
            if doi not in df_bibtex['doi'].tolist():
                print("Searching for DOI:", doi)
                result = cr.works(doi)
                if result['status'] == 'ok':
                    data = result['message']
                    flattened_data = flatten_crossref_data(data)
                    print("searched crossref via DOI and found", flattened_data)
                    if flattened_data['doi'] not in df_bibtex['doi'].to_list():
                        # Append the new entry to the existing DataFrame
                        df_bibtex = df_bibtex.append(flattened_data, ignore_index=True)
                    else:
                        print("DOI already in bibtex entries, not adding", row['Title'])
            else:
                pass
                print("DOI already in bibtex entries", row['Title'])
        else:
            # check to see if the entry is already in the bibtex entries by row['Authors']} {row['Title']} {row['Year']} {row['Source']
            matched_index_title = find_index(df_bibtex, row['Title'], "title")
            if not pd.isna(row['Source']):
                matched_index_source = find_index(df_bibtex, row['Source'], "title")
            else: 
                matched_index_source = None
            # # vendor_name = vendor_df.get_value(row,"Name of vendor")
            # string_length = len(row['Title'])
            # for columns in df_bibtex.index:
            #     doi_title = df_bibtex.at[columns, "title"][:string_length]
            #     matched_token=fuzz.partial_ratio(row['Title'],doi_title)
            #     print("matched_token", matched_token, row['Title'], doi_title)
            #     if matched_token> 80:
            #         matched_index = columns
            #         print("Matched index", matched_index)
            #         pass
            #         # matched_vendors.append([vendor_name,regulated_vendor_name,matched_token])

            if matched_index_title is not None:
                print("Matched index TITLE already crossrefed", matched_index_title, df_bibtex.loc[matched_index_title]["title"])
            elif matched_index_source is not None:
                print("Matched index SOURCE already crossrefed", matched_index_source, df_bibtex.loc[matched_index_source]["title"])
            else:
                # NO DOI
                is_match = False
                query = f"{row['Authors']} {row['Title']} {row['Year']} {row['Source']}"
                print("Searching for query:", query)
                result = cr.works(query=query)
                if result['status'] == 'ok':
                    data = result['message']['items'][0]
                    flattened_data = flatten_crossref_data(data)
                    print(flattened_data)
                    # test to see if the title OR source matches the resul
                    if fuzzy_test(row['Title'],flattened_data['title']):
                        print("Matched title", row['Title'], data['title'][0])
                        is_match = True
                    elif fuzzy_test(row['Source'],flattened_data['title']):
                        print("Matched SOURCE", row['Source'], data['title'][0])
                        is_match = True
                    else:
                        print("No match found for query:", query, flattened_data['title'])
                        # save to NO_DOI_FILE
                        with open(NO_DOI_FILE, 'a') as no_doi_file:
                            no_doi_file.write(f"{query}\n")
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


    # Write the combined entries back to JSON_FILE
    write_bibtex_entries(df_bibtex)

    return df_bibtex

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
    # print("parsing authors", authors)
    """
    Format author names from various input styles to 'Firstname Lastname' format.
    
    Args:
        authors (str): Authors as a string, with names separated by commas or semicolons
    
    Returns:
        str: Formatted author names in 'Firstname Lastname' format
    """
    def parse_name(name):
        # Remove extra whitespace
        name = name.strip()
        
        # Check if name contains a comma (last name, first name format)
        if ',' in name:
            # print("Comma in name", name)
            parts = name.split(',')
            return f"{parts[1].strip()} {parts[0].strip()}"
        
        # Standard first name last name format
        parts = name.split()
        return name if len(parts) <= 2 else f"{' '.join(parts[:-1])} {parts[-1]}"

    # Split authors by semicolon or comma, and parse each
    if ";" in authors:
        author_list = [author.strip() for author in authors.split(';')]
    elif "," in authors:
        author_list = [author.strip() for author in authors.split(',')]
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
    if pd.notna(row['pq_volume']) and row['pq_volume'] != 0:
        volume = row['pq_volume']
    elif pd.notna(row['Volume']) and row['Volume'] != 0:
        volume = row['Volume']
    else:
        volume = None

    if pd.notna(row['pq_issue']) and row['pq_issue'] != 0:
        issue = row['pq_issue']
    elif pd.notna(row['Issue']) and row['Issue'] != 0:
        issue = row['Issue']
    else:
        issue = None

    volume = float_to_int(volume)
    issue = float_to_int(issue)

    if volume is None and issue is None:
        return None
    elif volume is None:
        return f"({issue})"
    elif issue is None:
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

    if not pd.isna(row['Type']) and type(row['Type']) != float:
        if row['Type'].lower() == "book" or not pd.isna(row['Chapter']):
            is_book = True
        elif row['Type'].lower() == "edited volume" or not pd.isna(row['Chapter']) or not pd.isna(row['Chapter_Auth']):
            is_edited_volume = True

    # Convert all text fields to RTF Unicode escape sequences
    if not pd.isna(row['pq_Title']) and not row['pq_Title'].isspace():
        # using proquest data
        authors_col = 'pq_Authors'
        title_col = 'pq_Title'
        source_col = 'pq_pubtitle'
    else:
        authors_col = 'Authors'
        title_col = 'Title'
        source_col = 'Source'
    authors = unicode_to_rtf(row[authors_col])
    authors = format_authors(authors)
    title = unicode_to_rtf(row[title_col])
    source = unicode_to_rtf(row[source_col])
    Chapter_Auth = unicode_to_rtf(row['Chapter_Auth'])
    Chapter = unicode_to_rtf(row['Chapter'])

    volume_issue = format_volume_issue(row)
    # Start building the entry
    if show_year:
        cv_entry = f"{year}\\tab "
    else:
        cv_entry = "\\tab "

    # format entry based on type
    if volume_issue is not None:
        cv_entry += f"{authors}, \"{title},\" \\i {source}\\i0  {volume_issue}"
    elif is_book:
        cv_entry += f"{authors}, \\i {title}, \\i0 {source}"
    elif is_edited_volume:
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
            project_str = make_project_string(row)
            year = row['Year'] if pd.notna(row['Year']) else None
            
            # Determine if we should show the year
            show_year = year != current_year
            
            # Generate the entry
            cv_entry = make_cv_entry(row, project_str, show_year)
            section_entries.append(cv_entry)
            
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