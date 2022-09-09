import pandas as pd
import csv

fieldList = [
    "Folder", "Name", "Path", "SubjectArea", "Column",
    "Owner", "Created", "Creator", "Description", "DescriptionID",
    "Formula", "Modified", "Modifier", "Table"];

STMT_TEMPLATE = "INSERT INTO [BIReportToEDWLineage].[dbo].[OBIEE-Catalog-DUMP-Fixed] ([RowNum],[Folder],[Name],[Path],[SubjectArea],[Column],[Owner],[Created],[Creator],[Description],[DescriptionID],[Formula],[Modified],[Modifier],[Table]) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}')"

dropFormulaColumn = True;

def loadObieeCatalogCsv():
    fileName = 'OBIEE-Catalog-DUMP.csv'; #"OBIEE-Catalog-DUMP-Small.csv";#'OBIEE-Catalog-DUMP.csv'; #
    data = pd.read_csv(fileName, quotechar='"', doublequote=True, converters={i: str for i in range(0, 20)});
    return data;

def dealWithComma(value):
    if ("," in value):
        value = "\"" + value + "\"";
    return value;

def doubleQuote(value):
    value = "\"" + value + "\"";
    return value;

def dealWithNewLine(value):
    newLine = '\n';
    lineFeed = '\r';
    if (newLine in value or lineFeed in value):
        value = value.replace(newLine, ' ').replace(lineFeed, ' ')
    return value;
def replaceNewLines(data):
    #data["Formula"].replace(r'\\n', ' ', regex=True);
    #data["Formula"].replace(r'\\r', ' ', regex=True);
    data["Formula"].replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=[" "," "], regex=True, inplace=True);
    #print(data.columns);
    return data;

def main():
    data = loadObieeCatalogCsv();
    print("Size: ", len(data));
    fixedCatalogFile = open("OBIEE-Catalog-DUMP-Fixed-Direct.csv", "w", encoding="utf-8");
    if (dropFormulaColumn):
        print("Dropping the formula column");
        data.drop(['Formula'], axis = 1, inplace = True);
    else:
        data = replaceNewLines(data);
    data.to_csv(fixedCatalogFile,index=True, index_label='RowNumber', quoting=csv.QUOTE_ALL, line_terminator='\n');
    exit();

if __name__== "__main__":
  main()

