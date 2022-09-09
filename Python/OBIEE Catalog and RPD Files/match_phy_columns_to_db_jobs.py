import pandas as pd
from collections import defaultdict
from neo4j import GraphDatabase

setOfPhysicalLayerTableRefs = set();
bucketOfDatabaseSynonyms = defaultdict(set);
bucketOfPhysicalTables = defaultdict(set);
bucketOfSchemaTableJobs = defaultdict(set);
lineageBucketOfLists = defaultdict(dict);
tableSchemaGroupMap = {};
UNKNOWN_TABLE_SCHEMA_GROUP = "DBTable";
TABLE_SCHEMA_GROUP_NOT_FOUND = "Not_FOUND";

def addToBucket(bucket, key, dataTuple):
    bucket[key].add(dataTuple);

def addToLineageBucket(tableKey, itemKey, dataTuple):
    #entry = {};
    #entry[itemKey] = dataTuple;
    existingDictOfEntries = lineageBucketOfLists[tableKey];
    if (itemKey in existingDictOfEntries):
        return;
    else:
        existingDictOfEntries[itemKey] = dataTuple;

def lookupTableClass(schema):
    if (schema in tableSchemaGroupMap):
        tableSchemaGroup = tableSchemaGroupMap[schema];
    else:
        print("No class found for Schema ({0}), defaulting to {1}...".format(schema, UNKNOWN_TABLE_SCHEMA_GROUP));
        tableSchemaGroup = UNKNOWN_TABLE_SCHEMA_GROUP;
    return tableSchemaGroup;

def loadSchemaClassificationData():
    data = pd.read_csv('SchemaClassification.csv', keep_default_na=False);
    for i in data.index:
        tableClass = data['TableClassification'][i];
        tableSchema = data['TableSchema'][i];
        tableSchemaGroupMap[tableSchema] = tableClass;
    #print("Size of tableSchemaGroupMap;", len(tableSchemaGroupMap));
    #print("Keys of tableSchemaGroupMap;", set(tableSchemaGroupMap.values()));
    return tableSchemaGroupMap;

def loadSchemaTableJobData():
    data = pd.read_csv('SchemaTableJobDetails.csv', keep_default_na=False, dtype={'JobId':str});
    for i in data.index:
        tableClass = data['TableClassification'][i].strip();
        schema = data['Schema'][i];
        tableName = data['TableName'][i];
        jobId = data['JobId'][i];
        jobName = data['JobName'][i];
        relation = data['Relation'][i];
        addToBucket(bucketOfSchemaTableJobs, (schema, tableName), (tableClass, schema, tableName, jobId, jobName, relation));
        addToBucket(bucketOfPhysicalTables, (schema, tableName), (tableClass, schema, tableName));
        if (schema not in tableSchemaGroupMap):
            tableSchemaGroupMap[schema] = tableClass;

def loadDatabaseSynonymData():
    data = pd.read_csv('DatabaseSynonyms.csv', keep_default_na=False);
    for i in data.index:
        synOwner = data['SYNONYM_OWNER'][i];
        synName = data['SYNONYM_NAME'][i];
        phyTableOwner = data['TABLE_OWNER'][i];
        phyTableName = data['TABLE_NAME'][i];
        synonymTuple = (synOwner, synName);
        thePhyTupleKey = (phyTableOwner, phyTableName);
        tableClass = lookupTableClass(phyTableOwner);
        #if (thePhyTupleKey in bucketOfPhysicalTables):
        #    phyTableTuple = bucketOfPhysicalTables[thePhyTupleKey];
        #    tableClass = phyTableTuple[0];
        #    print("Matching physical table found for Synonym ", synonymTuple, " --> ", phyTableTuple);
        #else:
        #    print("No matching physical table found for Synonym ", synonymTuple, " with Physical mapping ", thePhyTupleKey);
        #    tableClass = TABLE_SCHEMA_GROUP_NOT_FOUND;
        addToBucket(bucketOfDatabaseSynonyms, synonymTuple, (synOwner, synName, tableClass, phyTableOwner, phyTableName));

def loadObieePhysicalLayerColumnsData():
    data = pd.read_csv('FullListOfPhysicalLayerTables.csv', keep_default_na=False);
    for i in data.index:
        phyDbName = data['PhysicalDbName'][i];
        phySchemaName = data['PhysicalSchemaName'][i];
        phyTableName = data['PhysicalTableName'][i];
        setOfPhysicalLayerTableRefs.add((phyDbName, phySchemaName, phyTableName));

TXT_OBIEEPhysicalLayerTableRef = "OBIEEPhysicalLayerTableRef";
TXT_DbSynonym = "DbSynonym";
TXT_DbPhysicalTable = "DbPhysicalTable";
TXT_ListOfJobs = "ListOfJobs";

def matchPhysicalLayerToDbJobs():
    for phyLayerTuple in setOfPhysicalLayerTableRefs:
        phyDbName = phyLayerTuple[0];
        phySchemaName = phyLayerTuple[1];
        phyTableName = phyLayerTuple[2];
        tableTuple = (phySchemaName, phyTableName);
        #if (phyTableName not in ["SYSTEM_ASUP", "SALES_ORDER_LINE_HOLD"]):
        #    continue;
        addToLineageBucket(tableTuple, TXT_OBIEEPhysicalLayerTableRef, phyLayerTuple);
        #print("Tuple: ", phyLayerTuple);
        # Check in the Synonym bucket
        if phySchemaName == "NULL":
            phySchemaName = "OBIEE_APP";
        theTuple = (phySchemaName, phyTableName);
        # Search in the bucket of Database Synonyms
        exactMatchFound = False;
        if (theTuple in bucketOfDatabaseSynonyms):
            synonymSet = bucketOfDatabaseSynonyms[theTuple];
            for synonym in synonymSet:
                synOwner = synonym[0];
                synName = synonym[1];
                tableClass = synonym[2];
                actualTableOwner = synonym[3];
                actualTableName = synonym[4];
                if (synOwner == phySchemaName):
                    print("Exact match found in Synonyms: ", phyLayerTuple, "->", synonym);
                    addToLineageBucket(tableTuple, TXT_DbSynonym, (synOwner, synName));
                    addToLineageBucket(tableTuple, TXT_DbPhysicalTable, (tableClass, actualTableOwner, actualTableName));
                    if ((actualTableOwner, actualTableName) in bucketOfSchemaTableJobs):
                        jobs = bucketOfSchemaTableJobs[(actualTableOwner, actualTableName)];
                        #addToLineageBucket(tableTuple, TXT_ListOfJobs, jobs);
                    else:
                        print("No matching jobs found in the bucketOfSchemaTableJobs for ", phyLayerTuple);
                    exactMatchFound = True;
                    break;
        if (exactMatchFound == False):
            # Search in the bucketOfPhysicalTables
            if (theTuple in bucketOfPhysicalTables):
                phyTableTuple = bucketOfPhysicalTables[theTuple];
                tableClass = phyTableTuple[0];
                exactMatchFound = True;
                print("Some match found in bucketOfPhysicalTables: ", phyLayerTuple, "->", phyTableTuple);
                addToLineageBucket(tableTuple, TXT_DbPhysicalTable, (tableClass, phySchemaName, phyTableName));
        if (exactMatchFound == False):
            print(phyLayerTuple, ": No match at all in the Synonyms or DB");

def getCypherSingleResponse(result, key):
    value = "";
    for line in result:
        value = line[key];
    return value;

def getCypherResponse(result, key):
    value = [];
    for line in result:
        value.append(line[key]);
    return value;


def getNodeId(result):
    return getCypherSingleResponse(result, "nodeId");

def getRelationIds(result):
    return getCypherResponse(result, "relationId");

def insertToNeo4J():
    print("Connecting to Neo4J");
    neo4jURI = "bolt://localhost:7687"
    neo4jDriver = GraphDatabase.driver(neo4jURI, auth=("neo4j", "netapp"));
    print("Connected to Neo4J. Getting the session object");
    neo4jSession = neo4jDriver.session();
    tx = neo4jSession.begin_transaction();
    print("Connected to Neo4J and got the session object. Now iterating through lineageBucketOfLists");
    for phyLayerTuple in lineageBucketOfLists:
        phySchemaName = phyLayerTuple[0];
        phyTableName = phyLayerTuple[1];
        #if (phyTableName not in ["SYSTEM_ASUP", "SALES_ORDER_LINE_HOLD"]):
        #    continue;
        values = lineageBucketOfLists[phyLayerTuple];

        if (TXT_DbSynonym in values):
            #
            # Create the DbSynonym node in Neo4J
            #
            synonymTuple = values[TXT_DbSynonym];
            synOwner = synonymTuple[0];
            synName = synonymTuple[1];
            synonymCreateStatement = "CREATE (n:DbSynonym {synonymOwner: \'" \
                + synOwner + "\', synonymName: \'" + synName + "\'}) RETURN ID(n) as nodeId";
            print(synonymTuple, ": synonymCreateStatement: [" + synonymCreateStatement + "]");
            returnResult = tx.run(synonymCreateStatement);
            dbSynonymNodeId = getNodeId(returnResult);
            print(synonymTuple, ": Created DbSynonym node with id: [" + str(dbSynonymNodeId) + "]");
        
            #
            # Create relationship from OBIEE Physical Layer node(s) to DbSynonym node
            # This is the example of such statement:
            #   MATCH (n:PhysicalColumn {physicalTableName: 'SYSTEM_ASUP', physicalSchemaName: 'OBIEE_APP', 
            #       physicalDbName: 'EDWNI_CORE'}),(m:DbSynonym) WHERE ID(m) = 40540
            #   CREATE (n)-[r:isColumnOf]->(m) RETURN ID(r) as relationId
            #
            phyLayerTuple = values[TXT_OBIEEPhysicalLayerTableRef];
            phyDbName = phyLayerTuple[0];
            phySchemaName = phyLayerTuple[1];
            phyTableName = phyLayerTuple[2];
            phyLayerNodeToSynonymRelationStmt = "MATCH (n:PhysicalColumn {physicalTableName: '" + phyTableName \
                + "', physicalSchemaName: '" + phySchemaName + "', physicalDbName: '" \
                + phyDbName + "'}), (m:DbSynonym) WHERE ID(m) = " + str(dbSynonymNodeId) \
                + " CREATE (n)-[r:isColumnOf]->(m) RETURN ID(r) as relationId";
            print(phyLayerTuple, ": phyLayerNodeToSynonymRelationStmt: [" + phyLayerNodeToSynonymRelationStmt + "]");
            returnResult = tx.run(phyLayerNodeToSynonymRelationStmt);
            phyLayerNodeToSynonymRelationIds = getRelationIds(returnResult);
            print(phyLayerTuple, ": Made relation to DbSynonym node ", \
                    synonymTuple, ", IDs: ", phyLayerNodeToSynonymRelationIds);

            if (TXT_DbPhysicalTable in values):
                #
                # Process the Physical database table part and create relationship from DbSynonym to
                # Physical database table. Example statement:
                #   MATCH (m:DbSynonym), (n:PEIM_Table {tableName: 'SYSTEM_ASUP', schemaName: 'FACTS'}) WHERE ID(m) = 40540 
                #       CREATE (m)-[r:mappedToPhysicalTable]->(n) RETURN ID(r) as relationId
                #
                phyDbTuple = values[TXT_DbPhysicalTable];
                tableClass = phyDbTuple[0];
                actualTableOwner = phyDbTuple[1];
                actualTableName = phyDbTuple[2];
                synonymToPhyDbTableRelationStmt = "MATCH (m:DbSynonym), (n:" + tableClass + " {schemaName: '" + actualTableOwner \
                    + "', tableName: '" + actualTableName + "'}) WHERE ID(m) = " + str(dbSynonymNodeId) \
                    + " CREATE (m)-[r:mappedToPhysicalTable]->(n) RETURN ID(r) as relationId";
                print(phyDbTuple, ": synonymToPhyDbTableRelationStmt: [" + synonymToPhyDbTableRelationStmt + "]");
                returnResult = tx.run(synonymToPhyDbTableRelationStmt);
                synonymToPhyDbTableRelationIds = getRelationIds(returnResult);
                print(phyDbTuple, ": Made relation to DbSynonym node ", \
                        synonymTuple, ", IDs: ", synonymToPhyDbTableRelationIds);
            else:
                print("Physical table details not found for ", phyLayerTuple, "... Skipping the linkage..");
        else:
            print("Synonym or Physical table details not found for ", phyLayerTuple, "... Skipping the linkage..");
        tx.commit();
        tx = neo4jSession.begin_transaction();
    tx.commit();
        
def main():
    tableSchemaGroupMap = loadSchemaClassificationData();
    jobTableData = loadSchemaTableJobData();
    synonymData = loadDatabaseSynonymData();
    obieePhysicalLayerColumnsData = loadObieePhysicalLayerColumnsData();
    print("setOfPhysicalLayerTableRefs size: ", len(setOfPhysicalLayerTableRefs));
    print("bucketOfDatabaseSynonyms size: ", len(bucketOfDatabaseSynonyms));
    print("bucketOfPhysicalTables size: ", len(bucketOfPhysicalTables));
    print("bucketOfSchemaTableJobs size: ", len(bucketOfSchemaTableJobs));
    matchPhysicalLayerToDbJobs();
    for key in lineageBucketOfLists:
        print("Key [", key, "]: [", lineageBucketOfLists[key], "]");
    insertToNeo4J();
    print("Done!!!");
    
if __name__== "__main__":
  main()
