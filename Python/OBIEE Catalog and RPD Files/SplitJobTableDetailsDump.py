import pandas as pd

RELATION_LOOKUP = {
    "INSERT": "INSERT",
    "SELECT": "SELECT",
    "UPDATE": "UPDATE",
    "TRUNCATE": "TRUNCATE",
    "DELETE": "DELETE",
    };

UNKNOWN_TABLE_SCHEMA_GROUP = "DBTable";

def loadJobTableDetailsCsv():
    data = pd.read_csv('JobTableDetails.csv', keep_default_na=False, dtype={'JobId':str, 'SQLKey': str});
    return data;

def loadSchemaClassificationData():
    data = pd.read_csv('SchemaClassification.csv', keep_default_na=False);
    tableSchemaGroupMap = {};
    for i in data.index:
        tableClass = data['TableClassification'][i];
        tableSchema = data['TableSchema'][i];
        tableSchemaGroupMap[tableSchema] = tableClass;
    #print("Size of tableSchemaGroupMap;", len(tableSchemaGroupMap));
    #print("Keys of tableSchemaGroupMap;", set(tableSchemaGroupMap.values()));
    return tableSchemaGroupMap;

def printJobRowMsg(jobId, jobName, msg):
    print("{0} (ID: {1}): {2}".format(jobName, jobId, msg));

def cleanupValue(input):
    return input.strip().replace("\"", "");

def getTableListForTruncate(jobId, jobName, tableNames):
    tableTupleList = [];
    tableList = tableNames.split(",");
    listSize = len(tableList);
    if len(tableList) == 0:
        return tableTupleList;
    if listSize % 2 != 0:
        print(jobName, " ---> Something wrong with table names [", tableNames, "]");
        return tableTupleList;
    index = 0;
    for i in range(0, listSize, 2):
        schema = cleanupValue(tableList[i]);
        table = cleanupValue(tableList[i + 1]);
        if (":2" == table):
            printJobRowMsg(jobId, jobName, "Skipping the table :2.");
            continue;
        tableTuple = (schema, table);
        tableTupleList.append(tableTuple);
    return tableTupleList;

def createReverseMap(data, tableSchemaGroupMap):
    schemalessTablesFile = open("SchemalessTables.csv", "w");
    schemaTableJobFile = open("SchemaTableJobDetails.csv", "w");
    count = 0;
    schemaTableJobFile.write("UID,TableClassification,Schema,TableName,JobId,JobName,Relation\n");
    schemalessTablesFile.write("JobId,JobName,SQLKey,TableNames\n");
    for i in data.index:
        jobId = data['JobId'][i];
        #print(i, " : jobId ", jobId);
        if (jobId is None) or (not jobId.isnumeric()):
            print("Skipping the row ", i, " since it contains non-numeric jobId ", jobId);
            continue;
        jobName = data['JobName'][i].strip();
        tableNames = data['TableNames'][i].strip();
        if len(tableNames) == 0:
            printJobRowMsg(jobId, jobName, "Skipping the job now since tableNames is empty.");
            continue;
        relation = data['SQLKey'][i].strip();
        if (relation == "ERROR"):
            print("Job [" + jobName + "] - SQL Key is ERROR");
            continue;
        relationType = RELATION_LOOKUP[relation];
        tableTupleList = [];
        if ("TRUNCATEX" == relationType):
            #tableTupleList = getTableListForTruncate(jobId, jobName, tableNames);
            printJobRowMsg(jobId, jobName, "Skipping the Truncate relationship for now.");
            continue;
        else:
            tableList = tableNames.split(",");
            for aTable in tableList:
                aTable = cleanupValue(aTable);
                if (":1" in aTable or ":2" in aTable):
                    printJobRowMsg(jobId, jobName, "Skipping the table with colon in the name - " + aTable);
                    continue;
                if ("%%TBL1%%" == aTable):
                    printJobRowMsg(jobId, jobName, "Skipping the table %%TBL1%%.");
                    continue;
                if ("DUAL" == aTable):
                    printJobRowMsg(jobId, jobName, "Skipping the table DUAL.");
                    continue;
                if (aTable.startswith("$EDW")):
                    if (aTable.startswith("$EDWNI_DIM_SCHEMA.")):
                        newValue = aTable.replace("$EDWNI_DIM_SCHEMA.", "DIMS.", 1);
                        printJobRowMsg(jobId, jobName, "Schema/table {0} replacing $EDWNI_DIM_SCHEMA with DIMS to get {1}, continuing...".format(aTable, newValue));
                        aTable = newValue;
                    else:
                        printJobRowMsg(jobId, jobName, "Schema starting with dollar sign ({0}), skipping...".format(aTable));
                        continue;
                if "." in aTable:
                    tableSchemaSplit = aTable.split(".");
                    schema = tableSchemaSplit[0];
                    table = tableSchemaSplit[1];
                else:
                    schema = "NONE";
                    table = aTable;
                    schemalessTablesFile.write("{0},{1},{2},{3}\n".format(jobId, jobName, relation, aTable));
                schema = cleanupValue(schema);
                table = cleanupValue(table);
                if (schema in tableSchemaGroupMap):
                    tableSchemaGroup = tableSchemaGroupMap[schema];
                else:
                    printJobRowMsg(jobId, jobName, "No class found for Schema ({0}), defaulting to {1}...".format(schema, UNKNOWN_TABLE_SCHEMA_GROUP));
                    tableSchemaGroup = UNKNOWN_TABLE_SCHEMA_GROUP;
                tableTuple = (tableSchemaGroup, schema, table);
                tableTupleList.append(tableTuple);
        for aTableTuple in tableTupleList:
            schemaTableJobFile.write("{0},{1},{2},{3},{4},{5},{6}\n".format(str(count), aTableTuple[0], aTableTuple[1], aTableTuple[2], jobId, jobName, relationType));
            count = count + 1;

    schemalessTablesFile.close();
    schemaTableJobFile.close();
    return data;

def main():
    data = loadJobTableDetailsCsv();
    tableSchemaGroupMap = loadSchemaClassificationData();
    createReverseMap(data, tableSchemaGroupMap);

if __name__== "__main__":
  main()

 
 