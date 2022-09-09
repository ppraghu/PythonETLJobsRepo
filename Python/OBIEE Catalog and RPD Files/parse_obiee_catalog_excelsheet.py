import pandas as pd
import csv

def replaceNewLines(data):
    #data["Formula"].replace(r'\\n', ' ', regex=True);
    #data["Formula"].replace(r'\\r', ' ', regex=True);
    data["Formula"].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" "," "], regex=True, inplace=True);
    #print(data.columns);
    return data;

dropFormulaColumn = True;

def main():
    fileName = 'OBIEE-Catalog-DUMP.xlsx'; #"OBIEE-Catalog-DUMP-Small.csv";#'OBIEE-Catalog-DUMP.csv'; #
    data = pd.read_excel(fileName);
    print("Size: ", len(data));
    fixedCatalogFile = open("OBIEE-Catalog-DUMP-Excel-Fixed.csv", "w", encoding="utf-8");
    if (dropFormulaColumn):
        print("Dropping the formula column");
        data.drop(['Formula'], axis = 1, inplace = True);
    else:
        data = replaceNewLines(data);
    data.to_csv(fixedCatalogFile,index=True, index_label='RowNumber', quoting=csv.QUOTE_ALL, line_terminator='\n');

if __name__== "__main__":
  main()
