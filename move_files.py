import os
import pandas as pd
import requests
import numpy as np

IS_TEST = False
IS_CLEANUP = False

# Load CSV data
test_file = 'test_data.csv'
af_file = 'af.csv'
input_file = 'search_data.csv'
output_path = 'cleaned_data_moving.csv'
documents_folder = 'documents_test'
subfolder = 'check'
move_copy = 'move'

if IS_CLEANUP:
    output_path = 'cleaned_data_moving_cleanup.csv'
    subfolder = 'cleanup'
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

# Read CSV

if IS_TEST:
    df = pd.read_csv(test_file)
else:
    df = pd.read_csv(input_file)
    # df_af = pd.read_csv(af_file)
    # df = pd.concat([df, df_af])

df = df.replace({np.nan: None})

print("total items", len(df))

if IS_CLEANUP:
    # drop rows where both df['Mandiberg'] and df['Check Fulltext'] match boolean
    # both are False
    df = df[(df['Mandiberg'] == False) & (df['Check Fulltext'] != True)]
else:
    # drop rows where both df['Mandiberg'] OR df['Check Fulltext'] match boolean
    # either are True
    df = df[(df['Mandiberg'] == True) | (df['Check Fulltext'] == True)]



    # df_true = df[df['Mandiberg'] == boolean_column]
    # df_fulltext = df[df['Check Fulltext'] == boolean_column]
    # df = pd.concat([df_true, df_fulltext])


# testing
df = df[df['Check Fulltext'] == True]


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


# apply make_filename to each row and assign the value to a column "local_filename"
merged_df = df_titles
merged_df['local_filename'] = merged_df.apply(make_filename, axis=1)

print("merged items", len(merged_df))
print(merged_df)



# Remove all instances of "…" from the field Abstract
# merged_df['Abstract'] = merged_df['Abstract'].str.replace('…', '')


# Download files if not already downloaded and mark 'downloaded'
def move_file(save_path, copy_path):
    # print(f"moving {save_path} to {copy_path}")
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
    

    #         response = requests.get(url, stream=True)
    #         if response.status_code == 200:
    #             with open(save_path, 'wb') as file:
    #                 for chunk in response.iter_content(chunk_size=8192):
    #                     file.write(chunk)
    #             print(f"Downloaded: {save_path}")
    #             return True
    #         else:
    #             print(f"Failed to download {url}: {response.status_code}")
    #             return False
    #     else:
    #         print(f"File already exists: {save_path}")
    #         return True
    # except Exception as e:
    #     print(f"Error downloading {url}: {e}")
    #     return False

# Add 'downloaded' column
merged_df['moved'] = False


# Process each row for FullTextURL downloads
for index, row in merged_df.iterrows():
    # print (row)
    # url = row.get('FullTextURL', '')
    # if pd.notna(url) and url.startswith('http'):
        # filename = f"{row['Authors'].replace(' ', '_').replace(',', '_')}_{row['Title'].replace(' ', '_')}.pdf"
        # filename = f"{row['Title'].replace(' ', '_')}.pdf"
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
