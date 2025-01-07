import os
import pandas as pd
import numpy as np

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

# Load CSV data
input_file = 'cleaned_data.csv'
cv_output_path = 'cv_entries.rtf'
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

# Read CSV
df = pd.read_csv(input_file)

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

def make_cv_entry(row, project_str, show_year=True):
    # print(row['Title'], row['Year'], row['ReviewType'],  project_str)
    if pd.notna(row['Year']):
        year = int(row['Year'])
    else:
        year = 1000
    
    # determine if book, or edited volume
    is_book = False
    is_edited_volume = False

    print(row['Title'], "type", row['Type'])

    if not pd.isna(row['Type']) and type(row['Type']) != float:
        if row['Type'].lower() == "book":
            is_book = True
        elif row['Type'].lower() == "edited volume":
            is_edited_volume = True

    # Convert all text fields to RTF Unicode escape sequences
    authors = unicode_to_rtf(row['Authors'])
    title = unicode_to_rtf(row['Title'])
    source = unicode_to_rtf(row['Source'])
    
    # Start building the entry
    if show_year:
        cv_entry = f"{year}\\tab "
    else:
        cv_entry = "\\tab "
    if is_book:
        cv_entry += f"{authors}, \\i {title}, \\i0 {source}"
    elif is_edited_volume:
        cv_entry += f"{authors}, \"{title},\" in \\i {source}\\i0"
    else:
        cv_entry += f"{authors}, \"{title},\" \\i {source}\\i0"
    
    # print(row['Title'], row['Volume'], row['Issue'], row['ReviewType'])
    if pd.notna(row['Volume']) and not row['Volume'].isspace():
        # is Volume a space character?
        print("[make_cv_entry] Volume is not NaN", row['Volume'])
        cv_entry += f", Volume {row['Volume']}"
    if pd.notna(row['Issue']):
        cv_entry += f", Issue {row['Issue']}"
    if row['ReviewType']:
        print("[make_cv_entry] ", row['Title'], row['ReviewType'])
        if row['ReviewType']==0 or row['ReviewType']=="0":
            print(row['Title'], "ReviewType is 0", row['ReviewType'])
            row['ReviewType']="TK"
        type_key = unicode_to_rtf(row['ReviewType'])
        print(type_key)
        type_str = TYPES_DICT.get(type_key, type_key)
        cv_entry += f", {type_str}"
        print(type_str)
    else:
        cv_entry += f", mention of"
    if project_str:
        project_str = unicode_to_rtf(project_str)
        # remove leading space
        if project_str[0] == " ":
            project_str = project_str[1:]
        cv_entry += f" {project_str}."
    return cv_entry

def sort_entries(entries_df):
    """Sort entries by year in reverse chronological order, putting NaN years at the end"""
    # Convert Year to float to handle NaN values
    entries_df['Year'] = pd.to_numeric(entries_df['Year'], errors='coerce')
    # Sort by Year descending, putting NaN at the bottom
    return entries_df.sort_values('Year', ascending=False, na_position='last')

# Add section header formatting
def format_section_header(section_name):
    return f"\\par\\par\\b {section_name}\\b0\\par"

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