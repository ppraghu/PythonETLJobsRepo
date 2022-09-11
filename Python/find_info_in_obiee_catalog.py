import pandas as pd
import csv

def load_obiee_catalog_csv():
    fileName = 'OBIEE-Catalog-DUMP.csv'; #"OBIEE-Catalog-DUMP-Small.csv";#'OBIEE-Catalog-DUMP.csv'; #
    data = pd.read_csv(fileName, quotechar='"', doublequote=True, converters={i: str for i in range(0, 20)});
    return data;

def main():
    data = load_obiee_catalog_csv();
    print("Size: ", len(data));
    for i in data.index:
        count = i + 1;
        folder = data["Folder"][i];
        if not folder.startswith("/shared"):
            print(count, ": ", folder[:10]);

if __name__== "__main__":
  main()

