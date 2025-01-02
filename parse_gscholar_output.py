import os
import pandas as pd
import requests
import numpy as np

IS_TEST = False
DO_DOWNLOAD = False

# Load CSV data
test_file = 'test_data.csv'
af_file = 'af.csv'
input_file = 'search_data.csv'
output_path = 'cleaned_data.csv'

def make_filename(row):
    filename =  f"{row['Authors'].replace(' ', '_').replace(',', '_')}_{row['Title'].replace(' ', '_')}.pdf"
    filename = filename.replace('?', '').replace(':', '').replace('/', '').replace('\\', '').replace('…', '...')
    filename = filename.replace('"', '').replace('*', '').replace('<', '').replace('>', '')
    filename = filename.replace('|', '').replace(' ', '_')
    filename = filename.replace("'", "")
    print(f"Filename: {filename}")
    return filename

# Read CSV

if IS_TEST:
    df = pd.read_csv(test_file)
else:
    df = pd.read_csv(input_file)
    df_af = pd.read_csv(af_file)
    df = pd.concat([df, df_af])

df = df.replace({np.nan: None})

print("total items", len(df))

# drop rows where column Mandiberg is FALSE
df_true = df[df['Mandiberg'] == True]
df_fulltext = df[df['Check Fulltext'] == True]
df = pd.concat([df_true, df_fulltext])

print("total TRUE items", len(df))

# 1. Remove exact duplicates
df = df.drop_duplicates()

# 2. Move Authors, Title, Year, Source to new df
df_titles_terms = df[['Authors', 'Title', 'Year', 'Source', 'Term']]
print("df_titles_terms", df_titles_terms)
df_titles = df[['Authors', 'Title', 'Year', 'Source']]
df_titles = df_titles.drop_duplicates()
print("deduped items", len(df_titles))
print("deduped items", (df_titles))

# for each row in df_titles, find every row in df_titles_terms that match the Authors, Title, Year, Source
# add the value of the "Term" column to a list
# add the list to a new column in df_titles

terms_list = []
for _, row in df_titles.iterrows():
    if row['Year'] is None:
        terms = df_titles_terms[
            (df_titles_terms['Authors'] == row['Authors']) & 
            (df_titles_terms['Year'].isnull())
        ]['Term'].tolist()
    else:
        terms = df_titles_terms[
            (df_titles_terms['Authors'] == row['Authors']) & 
            (df_titles_terms['Title'] == row['Title']) & 
            (df_titles_terms['Year'] == row['Year']) & 
            (df_titles_terms['Source'] == row['Source'])
        ]['Term'].tolist()
    #convert terms to a string, separated by commas
    terms = ", ".join(terms)

    terms_list.append(terms)

df_titles['Term'] = terms_list

print(df_titles)

# 3. for each row in grouped_df, find the first row in df that match the Authors, Title, Year, Source, and add that to merged_df
merged_df = pd.DataFrame(columns=df.columns)
for _, row in df_titles.iterrows():
    # print("\n\n new row")
    # print(row)

    if row['Year'] is None:
        matches = df[(df['Authors'] == row['Authors'])  & 
                    (df['Year'].isnull()) ]

    else:
    # matches = df[(df['Authors'] == row['Authors']) & 
    #              (df['Title'] == row['Title']) & 
    #              (df['Year'] == row['Year']) & 
    #              (df['Source'] == row['Source'])]
        matches = df[(df['Authors'] == row['Authors'])  & 
                    (df['Year'] == row['Year']) ]
    if not matches.empty:
        match = matches.iloc[0]
        # print("\n\n match")
        # print(match)
        merged_df = pd.concat([merged_df, match.to_frame().T], ignore_index=True)
    else:
        print(">>>>  No match found for row:")
        print(row)

# 4. for each row in merged_df, replace the Term with the combined Term from df_titles
merged_df['Term'] = df_titles['Term'].values

# apply make_filename to each row and assign the value to a column "local_filename"
merged_df['local_filename'] = merged_df.apply(make_filename, axis=1)

print("merged items", len(merged_df))
print(merged_df)



# Remove all instances of "…" from the field Abstract
merged_df['Abstract'] = merged_df['Abstract'].str.replace('…', '')

# Create 'documents' folder if it doesn't exist
documents_folder = 'documents'
os.makedirs(documents_folder, exist_ok=True)

# Download files if not already downloaded and mark 'downloaded'
def download_file(url, save_path):
    try:
        # Check if file already exists
        if not os.path.exists(save_path):
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(save_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                print(f"Downloaded: {save_path}")
                return True
            else:
                print(f"Failed to download {url}: {response.status_code}")
                return False
        else:
            print(f"File already exists: {save_path}")
            return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False

# Add 'downloaded' column
merged_df['downloaded'] = False


# Process each row for FullTextURL downloads
for index, row in merged_df.iterrows():
    url = row.get('FullTextURL', '')
    if pd.notna(url) and url.startswith('http'):
        # filename = f"{row['Authors'].replace(' ', '_').replace(',', '_')}_{row['Title'].replace(' ', '_')}.pdf"
        # filename = f"{row['Title'].replace(' ', '_')}.pdf"
        save_path = os.path.join(documents_folder, row['local_filename'])
        if DO_DOWNLOAD:
            if download_file(url, save_path):
                merged_df.at[index, 'downloaded'] = True

# Save the consolidated data to a new CSV
merged_df.to_csv(output_path, index=False)

print(f"Consolidated data saved to {output_path}")
