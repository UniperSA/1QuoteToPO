#Required packages
import time
import os
from collections import Counter #to avoid duplicasy
import tabula.io
import pandas as pd
#import re
#import pdfplumber
import fitz
#11162991 - AN-2402873 11.01.2024 14_13_34
#11162951 - Michalek_Austausch Schließanlagen Außentüren KW-Gebäude
#Reading PDFs data and converting to DataFrame
ft = time.time()
vendor = []
for file_name in os.listdir(r"C:\Users\S01854\OneDrive - Uniper SE\PR Attachments\GER\GER_Angebot_PDF"):
   if file_name.endswith(".pdf"):
    tables = tabula.read_pdf(rf"C:\Users\S01854\OneDrive - Uniper SE\PR Attachments\GER\GER_Angebot_PDF\{file_name}", pages='all', columns= [10], guess = False, stream = True, pandas_options={"header": None})
    if len(tables)>0:#scanned or non scanned pdf checked
        #Concatinating data from each page of PDF in a single dataframe
        df = pd.DataFrame()
        for i in range(len(tables)):
            Df = pd.concat([df, tables[i]], ignore_index = True, join = 'outer')
            df = Df
        #Data cleaning-eliminating unnecassary rows
        df.drop(0, axis = 1, inplace = True)
        #Renaming columns,all
        df.columns = ['PDFs Content']
        #to extract any of the strings such GmbH from each row
        pattern = r'(GmbH|AG|GmbH & Co.KG\.|ltd|GbR|GmbH & Co. KG).*\d{5}'
        pattern1 = r'(GmbH|AG|GmbH & Co.KG\.|ltd|GbR|GmbH & Co. KG)'
        # Function to extract the first occurrence of the pattern from a string
        df_vendor = df[df['PDFs Content'].str.contains(pattern, case=False, regex=True, na=False)]
        # Remove duplicates from the specified column
        df_vendor['PDFs Content'].drop_duplicates(inplace = True)
        df_vendor.dropna(inplace = True)
        
        # delete the empty list-nan one scanned pdf
        if len(list(df_vendor['PDFs Content']))==0:
            vendor.append("Vendor name Not extracted")
        else:
            vendor.append(list(df_vendor['PDFs Content'])[0])
        #vendor name & address extracted above
        #below are keywors that P2P are interested to be extracted
        #List of Keywords required to extract per P2P teams request-for non-uniper PO's
        s1 = "Angebot"
        s2 = "Angebotsdatum"
        s3 = "Versandart"
        s4 = "E-Mail"#vendor email
        s5 = "Datum" #synonym-do we need any datum for instance in Brenntag pdf file
        s6 = "Einzelpreis"
        s7 = "Gesamtpreis"
        s8 = "Bezeichnung"
        s9 = "Zahlungsbedingungen"
        s10 = "Lieferzeit" 
        s11 = "Artikel-Nr"
        s12 = "Menge"
        s13 = "Versand"
        s14 = "Lieferbedingung" 
        s15 = "Belegnummer"
        s16 = "Zahlungsvereinbarungen" #payment agreements 
        s17 = "KOSTENVORANSCHLAG" #synonym for Angebot
        s18 = "Vendor name"
        s19 = "Vendor Address"
        s20 = "@"
        s21 = "Lieferbedingungen"
        #Segragating keys into 3 categories- 1 is for Angebot and datum, email, incoterms all sysnoyms, 2 is data inside tables required to retries and 3 is vendor address and name
        Keys_1 = [s1,s2, s3, s4, s5, s17, s20]
        Keys_2 = [s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s21]
        Keys_3 = [s18, s1]
        
        #droping nan values
        df.dropna(axis=0, inplace = True)
        
        #Define a function to check for keywords in each row for dataframe which considers case insentivities and stripping the last charachter- auotmation rstrip strip right most charachter
        #element is keys & row is in dataframe
        def check_words(row, elements):
            row_words = {word.lower().rstrip(":-. ").strip() for word in str(row).split()}
            return any(element.lower() in row_words for element in elements)  # Check for elements in words
        
        #Apply the function to each row of dataframe- utilizing apply method   
        #After this operation one boolean column is added, whose true value depends on if the keyword is present in the row
        df['has_element'] = df['PDFs Content'].apply(check_words, elements=Keys_1)
        #Also store the index of all such rows using has-elements filtered True values
        Keyword_rows = df[df['has_element']]
        Keyword_row_numbers = Keyword_rows.index   #to get index
        
        #Since in some of the POs PDF the keyword value present in the next line, consider the row just after the the row that contain keyword
        def add_and_insert(numbers):
          new_list = []
          for i, num in enumerate(numbers):
            new_list.append(num)
            new_list.append(num + 1)
          return new_list
        #eliminate duplicate rows
        # _i_j: - 1 all other Angebot no, datum, email, etc, 2. table retrieve 3-vendor name and add
        #_i_j:  _j second 1 means: approach- everything to be in one line or going to the next line safe approach
        filtered_df_1_1 = df[df.index.isin(Keyword_row_numbers)]
        modified_list = list(Counter(add_and_insert(Keyword_row_numbers)))
        filtered_df_1_2 = df[df.index.isin(modified_list)]
        modified_list = list(Counter(add_and_insert(Keyword_row_numbers)))
        
        #has element column is dropped since its a boolean element and not needed
        filtered_df_1_1.drop('has_element', axis = 1, inplace= True)
        filtered_df_1_2.drop('has_element', axis = 1, inplace= True)
        #drop duplicacy
        filtered_df_1_1 = filtered_df_1_1.drop_duplicates()
        filtered_df_1_2 = filtered_df_1_2.drop_duplicates()
        #Extract_words function considers only the texts present after the keyword 
        def extract_words(text, keys):
          words = text.split()
          for i in range(len(words)):
              if i!= len(words)-1:
                 if words[i].lower().rstrip(" -.:").strip() in [word.lower() for word in keys]:
                    return " ".join(m for m in words[i:len(words)]) # m is any charchetr after keys
              else:
                  return words[i]
         
        #Apply function to extract text after the keyword 
        filtered_df_1_1['PDFs Content'] = filtered_df_1_1['PDFs Content'].apply(extract_words, keys = Keys_1)
        filtered_df_1_2['PDFs Content'] = filtered_df_1_2['PDFs Content'].apply(extract_words, keys = Keys_1)
        #Check for Duplicate rows
        filtered_df_1_1 = filtered_df_1_1.drop_duplicates()
        filtered_df_1_2 = filtered_df_1_2.drop_duplicates()
        #Count the number of words in each row
        word_count = filtered_df_1_1['PDFs Content'].str.split().str.len()
        # Filter rows where word count is greater than 1
        filtered_df_1_1 = filtered_df_1_1[word_count > 1]
        # adding Tabular data of those pdfs which have sturcture table using pdf plumber
        filtered_df_1_1.at[len(filtered_df_1_1), "PDFs Content"]=""
        filtered_df_1_2.at[len(filtered_df_1_2), "PDFs Content"]=""
        filtered_df_1_1.at[len(filtered_df_1_1)+1,"PDFs Content"]="table data"
        filtered_df_1_2.at[len(filtered_df_1_2)+1,"PDFs Content"]="table data"
        #Pymupdf methods to extract tables data and outputs in csv files
        pdf_document = fitz.open(rf"C:\Users\S01854\OneDrive - Uniper SE\PR Attachments\GER\GER_Angebot_PDF\{file_name}")

        # Iterate through each page, after knowing its nonscan pdf
        s = ""
        for page_number in range(pdf_document.page_count):
            page = pdf_document[page_number]
    
            # Get the text of the page
            page_text = page.get_text("text", sort= True)
            s+=page_text
            #print(s)
        #print(file_name)   
        filtered_df_1_1.at[len(filtered_df_1_1)+2, "PDFs Content"]=s
        filtered_df_1_2.at[len(filtered_df_1_2)+2, "PDFs Content"]=s
        
        # Convert the new row to a DataFrame--to add the first row as vendor name and address to filtered_df_1_1
        new_row_data = {'PDFs Content': vendor[-1]}
        new_row_df = pd.DataFrame([new_row_data])

        # Concatenate the new row DataFrame with the original DataFrame, and reset index
        filtered_df_1_1 = pd.concat([new_row_df, filtered_df_1_1]).reset_index(drop=True)
        filtered_df_1_2 = pd.concat([new_row_df, filtered_df_1_2]).reset_index(drop=True)
        # Save the DataFrame to a CSV file
        filtered_df_1_1.to_csv(rf"C:\Users\S01854\OneDrive - Uniper SE\PR Attachments\GER\GER_Angebot_csv\{file_name[0:len(file_name)-4]}_1.csv", index = False, header = False, escapechar='\\')
        filtered_df_1_2.to_csv(rf"C:\Users\S01854\OneDrive - Uniper SE\PR Attachments\GER\GER_Angebot_csv\{file_name[0:len(file_name)-4]}_2.csv", index = False, header = False, escapechar='\\')
print(time.time()-ft)
#----------------------------------------------


