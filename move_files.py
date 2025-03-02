import os
import pandas as pd
import requests
import numpy as np

# runs a test on a small dataset
IS_TEST = False

# I think this is for removing FALSE items where it doesn't mention or is only a citation
IS_CLEANUP = False

# Set these to True to only process one of the CSV files
# otherwise, it will process both
IS_AF_ONLY = False
IS_MM_ONLY = True

# Load CSV data
test_file = 'test_data.csv'
af_file = 'af.csv'
input_file = 'mm_all.csv'
output_path = 'cleaned_data_moving.csv'
documents_folder = 'documents'
subfolder = 'check'

# two options: 'move' or 'copy'
move_copy = 'move'

if IS_CLEANUP:
    if IS_AF_ONLY:
        subfolder = 'cleanup_af'
        output_path = 'cleaned_data_af_cleanup.csv'
    elif IS_MM_ONLY:
        subfolder = 'cleanup_mm'
        output_path = 'cleaned_data_mm_cleanup.csv'
    else:
        subfolder = 'cleanup'
        output_path = 'cleaned_data_moving_cleanup.csv'
    move_copy = 'move'
    # true_column = 'Check Fulltext'

# Create 'documents' folder if it doesn't exist
os.makedirs(documents_folder, exist_ok=True)
os.makedirs(os.path.join(documents_folder, subfolder), exist_ok=True)

def make_filename(row):
    filename =  f"{row['Authors'].replace(' ', '_').replace(',', '_')}_{row['Title'].replace(' ', '_')}.pdf"
    filename = filename.replace('?', '').replace(':', '').replace('/', '').replace('\\', '').replace('…', '...')
    filename = filename.replace('"', '').replace('*', '').replace('<', '').replace('>', '')
    filename = filename.replace('|', '').replace(' ', '_')
    filename = filename.replace("'", "")
    print(f"Filename: {filename}")
    return filename

def convert_to_bool(val):
    # It checks if the value, after converting to lowercase, is equal to 'true'. If so, it returns True, otherwise it returns False.
    return val.lower() == 'true'

# Read CSV

if IS_TEST:
    df = pd.read_csv(test_file)
elif IS_AF_ONLY:
    df = pd.read_csv(af_file, converters={"USE_THIS": convert_to_bool})
elif IS_MM_ONLY:
    df = pd.read_csv(input_file, converters={"USE_THIS": convert_to_bool})
else:
    df = pd.read_csv(input_file, converters={"USE_THIS": convert_to_bool})
    df_af = pd.read_csv(af_file, converters={"USE_THIS": convert_to_bool})
    df = pd.concat([df, df_af])

df = df.replace({np.nan: None})
# print(df.columns)
# df = df['USE_THIS'].apply(bool)
print(df)
print("total items", len(df))

# I don't remember what this does
if IS_CLEANUP:
    print("Cleaning up")
    print(df['USE_THIS'])
    # print(df[(df['USE_THIS'] == False)])
    print(df[(df['USE_THIS'] == True)])
    # only keep rows where df['USE_THIS'] is False
    # this will move the false ones to the cleanup folder
    df = df[(df['USE_THIS'] == False)]
elif IS_TEST:
    pass
else:
    # drop rows where both df['USE_THIS'] match boolean
    # either are True
    df = df[(df['USE_THIS'] == True)]



    # df_true = df[df['Mandiberg'] == boolean_column]
    # df_fulltext = df[df['Check Fulltext'] == boolean_column]
    # df = pd.concat([df_true, df_fulltext])


# testing
# df = df[df['Check Fulltext'] == True]


print("total TRUE items", len(df))

# this part removes duplicates, first by doing exact duplicates, then by removing duplicates based on Authors, Title, Year, Source
# 1. Remove exact duplicates
df = df.drop_duplicates()

# 2. Move Authors, Title, Year, Source to new df
df_titles_terms = df[['Authors', 'Title', 'Year', 'Source', 'Term']]
print("df_titles_terms", df_titles_terms)
df_titles = df[['Authors', 'Title', 'Year', 'Source']]
df_titles = df_titles.drop_duplicates()
print("deduped items", len(df_titles))
print("deduped items", (df_titles))


# apply make_filename to each row and assign the value to a column "local_filename"
merged_df = df_titles
merged_df['local_filename'] = merged_df.apply(make_filename, axis=1)
print("merged items", len(merged_df))
print(merged_df)

# moves or copies file from save_path to copy_path
# move or copy is determined by the move_copy variable
# checks if file exists at save_path before moving
def move_file(save_path, copy_path):
    print(f"moving {save_path} to {copy_path}")
    try:
        # Check if file exists
        if os.path.exists(save_path):
            # copy file to copy_path
            if not os.path.exists(copy_path):
                os.makedirs(os.path.dirname(copy_path), exist_ok=True)
                if move_copy == 'move':
                    os.rename(save_path, copy_path)
                elif move_copy == 'copy':
                    os.system(f"cp {save_path} {copy_path}")
                else:
                    print("Invalid move_copy value.")
                print(f"Copied to: {copy_path}")
            else:
                print(f"File already exists: {copy_path}")
            return True
        else:
            print(f"File does not exist: {save_path}")
            return False
    except Exception as e:
        print(f"Error moving {save_path}: {e}")
        return False
    
# Add 'moved' column
merged_df['moved'] = False


# Process each row and move file
for index, row in merged_df.iterrows():
    save_path = os.path.join(documents_folder, row['local_filename'])
    copy_path = os.path.join(documents_folder, subfolder, row['local_filename'])
    if move_file(save_path, copy_path):
        merged_df.at[index, 'moved'] = True
    else:
        # print(f"Failed to move {save_path}")
        merged_df.at[index, 'moved'] = False
# Save the consolidated data to a new CSV
merged_df.to_csv(output_path, index=False)

print(f"Consolidated data saved to {output_path}")
