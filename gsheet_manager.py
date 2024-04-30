import datetime
from datetime import datetime
import os
import gspread
import pandas as pd
import numpy as np
from datetime import date
import re

class gspreadsheet_manager:
    def __init__(self, gsheet=None):
        self.json_file_path = None
        self.spreadsheet_url = None
        self.gc = None
        self.doc = None
        self.worksheets = {}
        self.sheets_count = 0
    def set_json_path_and_url(self, json_file_path, gsheet_url):
        self.json_file_path = json_file_path
        self.spreadsheet_url = gsheet_url
    def open_spreadsheet(self):
        if (not self.json_file_path) or (not self.spreadsheet_url):
            return False
        try:
            self.gc = gspread.service_account(self.json_file_path)
            self.doc = self.gc.open_by_url(self.spreadsheet_url)
            return self.doc
        except Exception as e:
            print(f"Error occured opening sheet. {self.json_file_path} and {self.spreadsheet_url}")
            print(e)
            return False
    def open_worksheet(self, sheet_name):
        self.worksheets[sheet_name] = self.doc.worksheet(sheet_name)
        self.sheets_count += 1
        return self.worksheets[sheet_name]
    def update(self, which, where=None, what=None):
        if which in self.worksheets.keys():
            if not where:
                self.worksheets[which].update(where, self.data_preprocessing(what))
                return True
            else:
                self.worksheets[which].update(self.data_preprocessing(what))
                return True
        else:
            print(f"No such sheet:{which}")
            return False
    def update_sheet_with_df(self, which, what):
        return self.update(which, where=None, what=what)
    def data_preprocessing(self, df):
        df_copy = df.copy()  # Create a copy of the DataFrame to avoid modifying the original
        date_columns = [cn for cn in df_copy.columns.tolist() if 'date' in str(cn)]
        print(f"columns including date data: {date_columns}")
        for col in date_columns:
            df_copy[col] = df_copy[col].astype(str)
        #     # df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d')
        # convert datetime.date to string
        df_copy = df_copy.applymap(lambda x: str(x) if type(x) == datetime.date else x)
        # Convert int64 to int32
        df_copy = df_copy.applymap(lambda x: int(x) if type(x) == np.int64 else x)
        return df_copy
class sheet_manager_for_ffbe():
    def __init__(self):
        self.sheets = {}
        self.gm = gspreadsheet_manager()
        self.set_json_path()
        print(f"path: {self.json_path}")
        self.sheet_url = "https://docs.google.com/spreadsheets/d/1rSAyiMHyqeD-odGbJxF4TUlUZEMQp_so0Z6_2D6L6Hk/edit?pli=1#gid=1590214290"
        self.gm.set_json_path_and_url(self.json_path, self.sheet_url)
        print(self.gm.open_spreadsheet())
        self.score_sheet_name = 'test'
        self.score_df = None
        self.defender_list = []

    def set_json_path(self):
        par_path = os.path.abspath('..')
        json_file_name = r"board-for-ffbe-973785f1358b.json"
        json_file_name2 = r'board-for-ffbe-a9d6e94e060c.json'
        json_file_name3 = 'board-for-ffbe-e800c4b8d402.json'
        if os.path.exists(os.path.join(par_path, json_file_name)):
            self.json_path = os.path.join(par_path, json_file_name)
            return True
        elif os.path.exists(os.path.join(par_path, json_file_name2)):
            self.json_path = os.path.join(par_path, json_file_name2)
            return True
        elif os.path.exists(os.path.join(par_path, json_file_name3)):
            self.json_path = os.path.join(par_path, json_file_name3)
            return True
        elif os.path.exists(os.path.join('./', json_file_name)):
            self.json_path = os.path.join('./', json_file_name)
            return True
        elif os.path.exists(os.path.join('./', json_file_name2)):
            self.json_path = os.path.join('./', json_file_name2)
            return True
        elif os.path.exists(os.path.join('./', json_file_name3)):
            self.json_path = os.path.join('./', json_file_name3)
            return True
        else:
            print("No json file exists.")
            return False
    def open_sheets(self):
        sheets_to_open = [
            'other_stat', 'log', 'defender_board', 'attacker_board', 'score', 'test', 'defenders'
        ]
        for s in sheets_to_open:
            self.sheets[s] = self.gm.open_worksheet(s)
    def update_sheet_with_df(self, sheet_name, df):
        df_str = self.gm.data_preprocessing(df)
        return self.sheets[sheet_name].update([df_str.columns.tolist()] + df_str.values.tolist())
    def update_sheet_with_df_including_index(self, sheet_name, df):
        df_str = self.gm.data_preprocessing(df)
        print(df_str.dtypes)
        if len(df_str):
            df_to_write = df_str.fillna('')
            return self.sheets[sheet_name].update([[''] + df_to_write.columns.tolist()] + df_to_write.reset_index().values.tolist())
        else:
            return False
    def fetch_sheet_as_df(self, sheet_name):
        res = self.sheets[sheet_name].get_all_values()
        res_df = pd.DataFrame(res)
        return res_df
    def fetch_score_as_df(self):
        res = self.sheets[self.score_sheet_name].get_all_values()
        res[1] = map(lambda x: x.strip() if type(x) == str else x, res[1])
        date_p = re.compile('[월]+.*[일]+')
        res[1] = map(lambda x: self.convert_to_date(x) if re.findall(date_p, x) else x, res[1])
        res_df = pd.DataFrame(res[2:], columns=res[1])
        res_df.set_index('이름', inplace=True, drop=True)
        self.score_df = res_df
        self.defender_list = res_df.index.tolist()
    def convert_to_date(self, date_str):
        original_date = datetime.strptime(date_str, '%m월 %d일').date()
        new_date = date(2023, original_date.month, original_date.day)
        return new_date
if __name__ == '__main__':
    gm = sheet_manager_for_ffbe()
    gm.open_sheets()
    data = pd.DataFrame(np.random.randint(0,100,size=(3,5)))
    gm.update_sheet_with_df('test', data)
    gm.update_sheet_with_df_including_index('attackers', data)