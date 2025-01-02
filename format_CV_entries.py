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
EXCLUDE_TERMS = ["turbulece.org", "wikipedia", "eyebeam"]
SECTIONS = [
    "Historical Inclusion",
    "Case Studies",
    "Project and Exhibition Reviews",
    "Discussion",
    "Mentions"
]

# Read CSV
df = pd.read_csv(input_file)

def make_term_string(row):
    if pd.isna(row['Term']) or row['Term']==0 or row['Term']=="0":
        term_str = "_TK_"
        return term_str
    row['Term'] = list(set(row['Term'].split(", ")))
    # drop values "turbulece.org" and "wikipedia" from row['Term'] list
    row['Term'] = [term for term in row['Term'] if term not in EXCLUDE_TERMS]
    
    term_count = len(row['Term'])
    term_str = " "
    for i, this_term in enumerate(row['Term']):
        if not pd.isna(this_term):
            if term_count > 1 and i == term_count -1:
                term_str += "and "
            term_str += this_term
            if i < term_count - 1:
                term_str += ", "
    return term_str

def make_cv_entry(row, term_str, show_year=True):
    if pd.notna(row['Year']):
        year = int(row['Year'])
    else:
        year = "TK"
        
    # Convert all text fields to RTF Unicode escape sequences
    authors = unicode_to_rtf(row['Authors'])
    title = unicode_to_rtf(row['Title'])
    source = unicode_to_rtf(row['Source'])
    
    # Start building the entry
    if show_year:
        cv_entry = f".{year}\\tab "
    else:
        cv_entry = ". "
        
    cv_entry += f"{authors}, \"{title},\" \\i {source}\\i0"
    
    if pd.notna(row['Volume']):
        cv_entry += f", Volume {row['Volume']}"
    if pd.notna(row['Issue']):
        cv_entry += f", Issue {row['Issue']}"
    if row['Type.1']:
        if row['Type.1']==0 or row['Type.1']=="0":
            row['Type.1']=" TK "
        cv_entry += f" {unicode_to_rtf(row['Type.1'])}"
    else:
        cv_entry += f" mention"
    if term_str:
        cv_entry += f", {unicode_to_rtf(term_str)}"
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
        section_df = df[df['Type.1'] == "case study"].copy()
    elif section == "Project and Exhibition Reviews":
        section_df = df[df['Type.1'] == "review"].copy()
    elif section == "Discussion":
        section_df = df[df['Type.1'] == "discussion"].copy()
    else:  # Mentions
        section_df = df[df['Type.1'] == "mention"].copy()
    
    # Sort the section entries
    if not section_df.empty:
        section_df = sort_entries(section_df)
        
        # Group entries by year
        current_year = None
        
        # Generate entries for this section
        for _, row in section_df.iterrows():
            term_str = make_term_string(row)
            year = row['Year'] if pd.notna(row['Year']) else None
            
            # Determine if we should show the year
            show_year = year != current_year
            
            # Generate the entry
            cv_entry = make_cv_entry(row, term_str, show_year)
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
    cv_file.write('\\line'.join(all_entries))
    cv_file.write("\n}")

print(f"CV entries saved to {cv_output_path}")