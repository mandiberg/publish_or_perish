import pandas as pd
import numpy as np
import joblib
from thefuzz import fuzz, process
from joblib import Parallel, delayed

# print("pandas version::", pd.__version__)
# print("numpy version::", np.__version__)
# print("thefuzz version::", fuzz.__version__)
# print("joblib version::", joblib.__version__)

vendor_df = pd.DataFrame({"Name of vendor": ["FREDDIE AMERICAN GOURMET SAUCE", "CITYARCHRIVER 2018 FOUNDATION",
                                  "GLAXOSMITHKLINE CONSUMER HEALTHCARE 2020", "LACKEY SHEET METAL",
                                  "HELGET GAS PRODUCTS", "ORTHOQUEST", "PRIMUS STERILIZER COMPANY",
                                  "LACKEY SHEET,^METAL", "ORTHOQUEST LLC 18", "PRIMUS STERILIZER COMPANY,[LLC]"]})

regulator_df = pd.DataFrame({"Name of Entity": ["FREDDIE LEES AMERICAN GOURMET SAUCE", "CITYARCHRIVER 2015 FOUNDATION",
                                  "GLAXOSMITHKLINE CONSUMER HEALTHCARE", "FDA Company", "LACKEY SHEET METAL",
                                  "PRIMUS STERILIZER COMPANY LLC",
                                  "Great Bend  KS", "HELGET GAS PRODUCTS INC", "ORTHOQUEST LLC",
                                  "PRIMUS STERILIZER", "CITYARCHRIVER 2022 FOUNDATION"]})


matched_vendors = []

for row in vendor_df.index:
    vendor_name = vendor_df.at[row, "Name of vendor"]
    for columns in regulator_df.index:
        regulated_vendor_name = regulator_df.at[columns, "Name of Entity"]
        matched_token=fuzz.partial_ratio(vendor_name,regulated_vendor_name)
        if matched_token> 80:
            matched_vendors.append([vendor_name,regulated_vendor_name,matched_token])

print(matched_vendors)

# # Define the fuzzy metric (uncomment any one of the metric)
# # metric = fuzz.ratio
# # metric = fuzz.partial_ratio
# # metric = fuzz.token_sort_ratio
# metric = fuzz.token_set_ratio

# # Define Threshold for Metric
# thresh = 80

# ca = np.array(df1[["Company A"]])
# cb = np.array(df2[["Company B"]])

# # Parallel Code
# def parallel_fuzzy_match(idxa, idxb):
#     return [ca[idxa][0], cb[idxb][0], metric(ca[idxa][0], cb[idxb][0])]

# results = Parallel(n_jobs=-1, verbose=1)(delayed(parallel_fuzzy_match)(idx1, idx2) for idx1 in range(len(ca)) for idx2 in range(len(cb))
#                                          if (metric(ca[idx1][0], cb[idx2][0]) > thresh))

# # Sequential Code
# # from tqdm import tqdm
# # results = [(ca[idx1][0], cb[idx2][0], metric(ca[idx1][0], cb[idx2][0])) for idx1 in tqdm(range(len(ca))) for idx2 in range(len(cb)) if metric(ca[idx1][0], cb[idx2][0]) > thresh]

# results = pd.DataFrame(results, columns=["Company A", "Company B", "Score"])

# print(results)