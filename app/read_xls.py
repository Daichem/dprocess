'''
This file reads in a BL document and sets about appending the databases
'''
from xlrd import open_workbook
from db_management import *
from tqdm import tqdm
import tkinter as tk
from tkinter import filedialog

import pandas as pd
from sqlalchemy import create_engine

# from tkinter import Button, Tk, HORIZONTAL

from tkinter.ttk import Progressbar
import sys
# import time
# import threading

class file_reader(tk.Tk):

    def __init__(self):
        super().__init__()
        self.button = tk.ttk.Button(text="Start",command=lambda : self.start_reader())
        self.button.pack()
        self.progress = tk.ttk.Progressbar(orient="horizontal", length=200, mode="determinate")
        self.progress.pack()
    def close(self):
        self.destroy()
        sys.exit()
    def start_reader(self):
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.askopenfilename()
        # file_path = "data.xlsx"
        new_manager = manager()
        wb = open_workbook(file_path)
        for sheet in wb.sheets():
            number_of_rows = sheet.nrows
            number_of_columns = sheet.ncols

            items = []

            rows = []
            column_names = []
            self.progress.start()
            self.progress['maximum'] = 100
            for row in range(0, number_of_rows):
                values = []
                for col in range(number_of_columns):
                    value = (sheet.cell(row, col).value)
                    try:
                        value = str(int(value))
                    except ValueError:
                        pass
                    finally:
                        values.append(value)
                if row == 0:
                    column_names = values
                    hs_code_idx = column_names.index('HS code')
                    seller_idx = column_names.index('Supplier')
                    buyer_idx = column_names.index('Buyer')
                    buyer_address_idx = column_names.index('Address2')
                    seller_address_idx = column_names.index('Address')
                    import_port_idx = column_names.index('Import port')
                    export_port_idx = column_names.index('Export port')
                    buyer_email_idx = column_names.index('E-Mail')
                    buyer_POC_idx = column_names.index('Contact name')
                    buyer_phone_idx = column_names.index('Contact tel')
                    description_idx = column_names.index('Product Description')
                else:
                    company(name=values[seller_idx], address=values[seller_address_idx])  # seller
                    company(name=values[buyer_idx], address=values[buyer_address_idx], POC=values[buyer_POC_idx],
                            phone=values[buyer_phone_idx], email=values[buyer_email_idx])  # purchaser
                    product(HS_code=values[hs_code_idx], description=values[description_idx])  # product information
                    port(name=values[import_port_idx])  # import port
                    port(name=values[export_port_idx])  # export port
                    if values[hs_code_idx] == "" or values[description_idx] == "":
                        continue
                    else:
                        order(buyer=values[buyer_idx], seller=values[seller_idx], HS_code=values[hs_code_idx],
                              export_port=values[export_port_idx], import_port=values[import_port_idx])  # order information
                self.progress['value'] = (row + 1) / number_of_rows*100
                self.progress.update()
            self.progress.stop()
        engine = create_engine('sqlite:///' + new_manager.db_name)
        tables = ['company', 'product', 'port']
        file_names = ['companies', 'products', 'ports']
        for i in range(len(tables)):
            df = pd.read_sql_table(tables[i], engine)
            df.to_excel(file_names[i] + ".xlsx")

        self.close()

if __name__== "__main__":
    FR = file_reader()
    FR.mainloop()
    FR.quit()