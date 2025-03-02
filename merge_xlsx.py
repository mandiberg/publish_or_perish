import pandas as pd
import numpy as np
import glob
import os

all_data = pd.DataFrame()
base = "/Users/michaelmandiberg/Documents/GitHub/publish_or_perish/full_bibliography/AF_proquest/"
# all_files = glob.glob("/Users/michaelmandiberg/Documents/GitHub/publish_or_perish/full_bibliography/AF_proquest/*.xlsx")
all_files = os.listdir(base)
# create a list of all files in the directory
# all_files = glob.glob("/Users/michaelmandiberg/Documents/GitHub/publish_or_perish/full_bibliography/AF_proquest/*.xlsx")
print(all_files)
if all_files:
    all_data = pd.concat((pd.read_excel(os.path.join(base, f)) for f in all_files), ignore_index=True)
    columns_to_keep = [
        'Title', 'Abstract', 'StoreId', 'AccessionNumber', 'AlternateTitle', 'ArticleType', 'AuthorAffiliation', 
        'Authors', 'InvestigatorCollaborator', 'companies', 'copyright', 'digitalObjectIdentifier', 'documentType', 
        'elecPubDate', 'entryDate', 'identifierKeywords', 'isbn', 'issn', 'issue', 'language', 'languageOfSummary', 
        'originalTitle', 'pages', 'placeOfPublication', 'pubdate', 'pubtitle', 'year', 'publisher', 'sourceAttrib', 
        'volume', 'DocumentURL', 'URL', 'FindACopy', 'Database'
    ]
    all_data = all_data[columns_to_keep]
    writer = pd.ExcelWriter('/Users/michaelmandiberg/Documents/GitHub/publish_or_perish/full_bibliography/merged.xlsx', engine='xlsxwriter')
    all_data.to_excel(writer, sheet_name='Sheet1')
    writer.save()
else:
    print("No files found to concatenate.")