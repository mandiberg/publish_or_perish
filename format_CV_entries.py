import os
import pandas as pd
import numpy as np


# Load CSV data
input_file = 'cleaned_data.csv'
cv_output_path = 'cv_entries.rtf'

# Read CSV
df = pd.read_csv(input_file)
# df = df.replace({np.nan: 0})

# Generate CV entries
cv_entries = []
for _, row in df.iterrows():
    if pd.notna(row['Year']):
        year = int(row['Year'])
    else:
        year = "TK"

    if pd.isna(row['Term']):
        term_str = None
    else:
        row['Term'] = row['Term'].split(", ")
        print(row['Term'])
        print(type(row['Term']))
        print(len(row['Term']))

        term_count = len(row['Term'])
        term_str = ""
        for i, this_term in enumerate(row['Term']):
            if not pd.isna(this_term):
                print(i, this_term)
                if term_count > 1 and i == term_count -1:
                    term_str += "and "
                term_str += this_term
                if i < term_count - 1:
                    term_str += ", "




    cv_entry = f".{year}\\tab {row['Authors']}, \"{row['Title']}\", \\i {row['Source']}\\i0"
    if pd.notna(row['Volume']):
        cv_entry += f", Volume {row['Volume']}"
    if pd.notna(row['Issue']):
        cv_entry += f", Issue {row['Issue']}"
    if term_str:
        cv_entry += f", {term_str}"
    # cv_entry += "\\line"
    print(cv_entry)
    cv_entries.append(cv_entry)
print(cv_entries)
# Save CV entries to an RTF file
with open(cv_output_path, 'w') as cv_file:
    cv_file.write("{\\rtf1\\ansi\\deff0\n")
    cv_file.write('\\line\\line'.join(cv_entries))
    cv_file.write("\n}")

print(f"CV entries saved to {cv_output_path}")
