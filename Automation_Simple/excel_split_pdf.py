import pandas as pd
import os
import win32com.client as win32

# Load the Excel file
input_file = "Input File Path here"
df = pd.read_excel(input_file)

# Export each sheet to PDF using Excel COM interface
excel = win32.gencache.EnsureDispatch('Excel.Application')
wb = excel.Workbooks.Open(os.path.abspath(input_file))

for i in range(7):
    sheet = wb.Worksheets(f'Sheet{i+1}')

    # Ensure all columns fit to one page wide
    sheet.PageSetup.Zoom = False
    sheet.PageSetup.FitToPagesWide = 1
    sheet.PageSetup.FitToPagesTall = False  # Allow any number of vertical pages
    # Optional: Landscape mode (recommended if many columns)
    sheet.PageSetup.Orientation = 2 

    pdf_path = os.path.abspath(f'C:\\Users\\ac145044\\Downloads\\Sheet{i+1}.pdf')
    sheet.ExportAsFixedFormat(0, pdf_path)

wb.Close(False)
excel.Quit()
