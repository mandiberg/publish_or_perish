This pipeline processes CSV files saved from the Publish or Perish software. The goal is to assemble a CV entry formatted text from the raw data. 

Order of operations:
0. TK needs to work backwards from existing CV to deconstruct it into structured data to ingrate
0. TK needs to work backwards from folder of PDFs to extract structured data to integrate
1. PoP pulls queries from Google Scholar
2. Add the search Term to the sheet in Excel (could probably be done with py but I did in Excel)
3. parse_gscholar_output.py removes duplicates, and fetches PDFs with reqeusts (succeeded for about 60%)
4. Manually DL the remaining PDFs (UGH) and be sure to save with the local_filename created in the CSV output
5. Drop PDFs into Notebook LM and use the N.LM prompt to assess whether/how much the PDFs actually discuss the topic at play
6. TK format_CV_entries.py reads the CSV, and outputs formatted CV entries, sorted based on category of engagement
7. TK copy_PDFs_to_folders.py creates folders of files for each category of engagement
