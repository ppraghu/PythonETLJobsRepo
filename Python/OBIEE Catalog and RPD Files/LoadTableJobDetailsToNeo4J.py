import pandas as pd
from neo4j import GraphDatabase


def loadSchemaTableJobDetailsCsv():
    data = pd.read_csv('SchemaTableJobDetails.csv', dtype={'SQLKey': str});
    return data;

def createSets(data):
    tableSet = set();
    jobSet = set();
    jobTableRelationSet = set();
    for i in data.index:
        #print("Processing row {0}: {1}".format(i, data.iloc[i]));
        # CSV Format: UID,Schema,TableName,JobId,JobName,Relation
        jobId = data['JobId'][i];
        jobName = data['JobName'][i].strip();
        jobSet.add((jobId, jobName));
        tableClass = data['TableClassification'][i].strip();
        schema = data['Schema'][i].strip();
        table = data['TableName'][i].strip();
        tableSet.add((tableClass, schema, table));
        relation = data['Relation'][i].strip();
        jobTableRelationSet.add((jobName, tableClass, schema, table, relation));
    print("Sizes: TableSet({0}), JobSet({1}), jobTableRelationSet({2})".format(len(tableSet), len(jobSet), len(jobTableRelationSet)));
    return tableSet, jobSet, jobTableRelationSet;

def populateTableNodes(session, tableSet):
    count = 1;
    tx = session.begin_transaction();
    for val in tableSet:
        tableClass = val[0];
        schema = val[1];
        table = val[2];
        createStatement = "CREATE (n:" + tableClass + " {tableName: \'" + table + "\', schemaName: \'" + schema + "\'})";
        print(createStatement);
        tx.run(createStatement);
        if (count % 100 == 0):
            print("populateTableNodes: Committing the tx after ", count, " iterations");
            tx.commit();
            tx = session.begin_transaction();
        count = count + 1;
    tx.commit();

def populateJobNodes(session, jobSet):
    count = 1;
    tx = session.begin_transaction();
    for val in jobSet:
        jobIdStr = "{0}".format(val[0]);
        createStatement = "CREATE (n:Job {jobName: \'" + val[1] + "\', jobId: " + jobIdStr + "})";
        print(createStatement);
        tx.run(createStatement);
        if (count % 100 == 0):
            print("populateJobNodes: Committing the tx after ", count, " iterations");
            tx.commit();
            tx = session.begin_transaction();
        count = count + 1;
    tx.commit();

def populateRelationships(session, jobTableRelationSet):
    count = 1;
    tx = session.begin_transaction();
    for val in jobTableRelationSet:
        jobName = val[0];
        tableClass = val[1];
        schema = val[2];
        table = val[3];
        relation = val[4];
        matchStatement = "MATCH (schema: " + tableClass + "), (j: Job)";
        whereClause = "WHERE schema.tableName = \"" + table + "\" AND schema.schemaName = \"" \
                + schema + "\" AND j.jobName = \"" + jobName + "\"";
        relationStatement =  "CREATE (j)-[r: " + relation + "]->(schema) RETURN schema, j";
        createStatement = matchStatement + " " + whereClause + " " + relationStatement;
        print(createStatement);
        tx.run(createStatement);
        if (count % 100 == 0):
            print("populateRelationships: Committing the tx after ", count, " iterations");
            tx.commit();
            tx = session.begin_transaction();
        count = count + 1;
    tx.commit();

def connectToNeo4J():
    neo4jURI = "bolt://localhost:7687"
    driver = GraphDatabase.driver(neo4jURI, auth=("neo4jxx", "xxxyyyy"));

def main():
    data = loadSchemaTableJobDetailsCsv();
    neo4jURI = "bolt://localhost:7687"
    driver = GraphDatabase.driver(neo4jURI, auth=("neo4jxx", "xxxyyyy"));
    tableSet, jobSet, jobTableRelationSet = createSets(data);
    #populateTableNodes(driver, tableSet);
    with driver.session() as session:
        populateTableNodes(session, tableSet);
        populateJobNodes(session, jobSet);
        populateRelationships(session, jobTableRelationSet);

if __name__== "__main__":
  main()


