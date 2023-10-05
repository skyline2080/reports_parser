
import pandas as pd
import glob

import re

from functools import partial, reduce
from typing import Callable, Mapping, List
from os.path import basename, splitext 


MONTHS = {  
    'декабрь'   : 12,
    'ноябрь'    : 11,
    'октябрь'   : 10,
    'сентябрь'  : 9,
    'август'    : 8,
    'июль'      : 7,
    'июнь'      : 6, 
    'май'       : 5, 
    'апрель'    : 4, 
    'март'      : 3, 
    'февраль'   : 2, 
    'январь'    : 1,


}

def foreach (fn, it):
    for elem in it: fn (elem)

pipe    = lambda *fns: lambda v: reduce (lambda acc, fn: fn (acc), fns, v)
curry   = lambda fn, n = 2: lambda v: fn (v) if n == 1 else curry (partial (fn, v), n - 1)
curry_3 = lambda fn: curry (fn, 3)
starred = lambda fn: lambda args: fn (*args)

head    = lambda ls: ls [0]



open_excel_file             = partial (pd.ExcelFile, engine= 'openpyxl')
take_exact_sheet_name       = lambda sheet_name: lambda ef: sheet_name 

def take_the_only_sheet_name   (ef: pd.ExcelFile): return head (ef.sheet_names)

def take_sheet_of_latest_month (df: pd.ExcelFile): 
    return (
        head (
            sorted (
                df.sheet_names, 
                key= lambda sheet_name: MONTHS.get (sheet_name.lower(), -1),
                reverse= True)
                
        )
    
    )


def take_sheet_of_latest_year_month (df: pd.ExcelFile):
    def key_logic (sheet_name: str):
        if ' ' in sheet_name: 
            month, year = sheet_name.split(' ')
            return (int(year), MONTHS.get(month.lower(), - 1))
        else:
            return (-1, MONTHS.get(sheet_name.lower(), - 1))

    return (
        head (
            sorted (
                df.sheet_names, 
                key= key_logic,
                reverse= True)
                
        )
    
    )



@curry_3
def parse (skiprows: int, choose_sheet_name: Callable[[pd.ExcelFile], str], df: pd.ExcelFile):
    chosen_name = choose_sheet_name(df)
    if __debug__: print (df.io.capitalize (), chosen_name)

    return df.parse (
            sheet_name= chosen_name, 
            skiprows= skiprows,
            parse_dates= False

        )



def drop_unused_columns (df: pd.DataFrame): return df.loc [:, ~ df.isna().all(axis= 0)]
def drop_unused_indeces (df: pd.DataFrame): return df.loc [~ df.isna().all(axis= 1), :]

drop_columns    = lambda *idx: lambda df: df.iloc [:, [i for i in range (df.columns.size) if i not in idx]]
drop_last_rows  = lambda n: lambda df: df[:-n]
drop_first_rows = lambda n: lambda df: df[n: ]



MAPPER = {
    '.*РУМ_ОПЕКА.*'     : pipe (open_excel_file, parse(1)(take_sheet_of_latest_month),                      drop_unused_columns, drop_unused_indeces),
    '.*РУМ_НЕОФАРМ.*'   : pipe (open_excel_file, parse(2)(take_sheet_of_latest_month),                      drop_unused_columns, drop_unused_indeces, drop_first_rows (2)),
    '.*П-17.*'          : pipe (open_excel_file, parse(1)(take_exact_sheet_name('форма_торги_template')),   drop_unused_columns, drop_unused_indeces, drop_first_rows (1)),
    '.*П-14.*'          : pipe (open_excel_file, parse(1)(take_exact_sheet_name('форма_торги')),            drop_unused_columns, drop_unused_indeces, drop_last_rows  (1)),
    '.*П-12.*'          : pipe (open_excel_file, parse(1)(take_exact_sheet_name('форма_торги_template')),   drop_unused_columns, drop_unused_indeces, drop_first_rows (2)),
    '.*П-10.*'          : pipe (open_excel_file, parse(8)(take_the_only_sheet_name),                        drop_unused_columns, drop_unused_indeces, drop_last_rows  (1)),
    '.*П-07.*'          : pipe (open_excel_file, parse(1)(take_sheet_of_latest_year_month),                 drop_unused_columns, drop_unused_indeces, drop_last_rows  (1)),
    '.*П-3.*'           : pipe (open_excel_file, parse(1)(take_exact_sheet_name('форма_торги')),            drop_unused_columns, drop_columns (0), drop_unused_indeces),


}



@curry
def fetch_processing_pipe (mapper: Mapping, fname: str):
    for pat in mapper:
        if re.match(pat, fname):
            return mapper [pat]
            
    raise ValueError()    
    

def parse_files (dir: str) -> Mapping [str, pd.DataFrame]:
    extract_filename = lambda fname: head (splitext (basename (fname)))
    files            = glob.glob(f'{dir}\*.xlsx', recursive= False)

    return { 
        extract_filename (fname): 
            fetch_processing_pipe (MAPPER) (fname)      # для каждого файла ищем соотв. схему обработки
                (fname)                                 # и вызываем ее для этого же файла

        for fname in files 

}



if __name__ == '__main__':
    PATH = r'C:\Users\RYU_ANTONOV\Desktop\Новая папка (2) — копия'

    dfs = parse_files (PATH)

    for name, df in dfs.items():
        print(name)
        print(df)
        print()
        print()

