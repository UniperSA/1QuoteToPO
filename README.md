# Text Extraction (for Quotations, POs)

## Overview
This Python script automates the extraction of key information from Purchase Orders (POs) in PDF format. It handles both scanned and non-scanned PDFs, segregating the extracted data into three main categories: vendor details, essential PO information, and table data.

## Features
- **PDF Handling:** Supports only non-scanned PDFs.
- **Data Extraction:** Extracts vendor names, important dates, pricing details, delivery terms, and other essential information from POs.
- **Data Segregation:** Segregates extracted data into vendor details, PO-specific information, and table data.
- **Keyword Filtering:** Utilizes keyword-based filtering to identify relevant information in POs.
- **CSV Output:** Outputs the extracted data into structured CSV files for further analysis.

## Dependencies
- Python 3.10
- pandas
- tabula-py
- PyMuPDF
- numpy

# Usage
Connected to the POs in sharepoint: 
[Link to PR Attachments Folder](https://uniper.sharepoint.com/:f:/r/sites/OGrp_SpotBuyTeam/Shared%20Documents/General/1QuoteToPO/PR%20Attachments?csf=1&web=1&e=mfP4Mb)
Run the main script: 
1QuoteTOPO_REv.03.py
Follow the prompts to process the POs and extract the necessary information.
The extracted data will be saved in CSV files in the specified output directory.

# Sample Output
csv files: Contains extracted vendor names and addresses also contains extracted PO-specific information, and contains extracted table data from POs.

# Contributing
Contributions are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request.

# License
This project is licensed under the MIT License.
