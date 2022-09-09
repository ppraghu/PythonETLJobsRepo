import pyodbc
from neo4j import GraphDatabase

processFullDashboardReports = True;

reportsOfInterest = [
    #"/shared/ENTERPRISE ANALYTICS/Business Review Analytics/Business Review Summary/BR - Sales Booking Overview",
    #"/shared/FUNCTIONAL ANALYTICS/Q2I Pricing/PVR Analysis/PVR''s Volume by Initiation date",
    #"/shared/ENTERPRISE ANALYTICS/Business Review Analytics/Account Analysis/BR - Customer Concentration",
    #"/shared/ENTERPRISE ANALYTICS/Business Review Analytics/Account Analysis/BR - Major Account 1 Bookings Analysis",
    #"/shared/ENTERPRISE ANALYTICS/Business Review Analytics/Account Analysis/BR - Sales Booking Trend by Acct Classification & Geo",
    #"/shared/ENTERPRISE ANALYTICS/Business Review Analytics/Account Analysis/BR - SI and Service Provider Analysis"
    "/shared/ENTERPRISE ANALYTICS/Business Review Analytics/Secondary KPI Analysis/BR-Active\/Past Due Pipeline Trending Analysis"

    ];

fullSelectStmt = ("SELECT DISTINCT cat.[Name] as ReportName, "
+ "cat.[Path] as ReportPath, cat.[SubjectArea] as CatalogSubjectArea, "
+ "cat.[Table] as CatalogTableName, cat.[Column] as CatalogColumnName, pres.[SubjectArea] as PresentationSubjectArea, "
+ "pres.[TableName] as PresentationTableName, pres.[ColumnName] as PresentationColumnName, "
+ "pres.[BusinessLayerMapping] as BusinessLayerMappingInPresLayer, "
+ "bus.[PresentationLayerMapping] as PresLayerMappingInBusinessLayer, "
+ "bus.[BusinessModelName] as BusinessModelName, bus.[TableName] as BusLayerLogicalTableName, "
+ "bus.[ColumnName] as BusLayerLogicalColumnName, bus.[PhysicalLayerMapping] as PhysicalLayerMappingInBusinessLayer, "
+ "phy.[BusinessLayerMapping] as BusinessLayerMappingInPhysicalLayer, "
+ "phy.[DatabaseName] as PhysicalDbName, phy.[SchemaName] as PhysicalSchemaName, phy.[TableName] as PhysicalTableName, "
+ "phy.[ColumnName] as PhysicalColumnName FROM "
+ "[BIReportToEDWLineage].[dbo].[OBIEECatalogLayerData] cat "
+ "LEFT OUTER JOIN [BIReportToEDWLineage].[dbo].[PresentationLayerColumns] pres "
+ "ON cat.[SubjectArea] = pres.[SubjectArea] AND cat.[Table] = pres.[CatalogTableRef] AND cat.[Column] = pres.[ColumnName] "
+ "LEFT OUTER JOIN [BIReportToEDWLineage].[dbo].[BusinessLayerColumns] bus "
+ "ON pres.[BusinessLayerMapping] = bus.[PresentationLayerMapping] "
+ "LEFT OUTER JOIN [BIReportToEDWLineage].[dbo].[PhysicalLayerColumns] phy "
+ "ON bus.[PhysicalLayerMapping] = phy.[BusinessLayerMapping] "
+ "WHERE cat.[SubjectArea] is NOT NULL AND cat.[Path] IN "
+ "({0}) ORDER BY cat.[Path]");

resultFieldsList = ["ReportName", "ReportPath", "CatalogSubjectArea", "CatalogTableName", "CatalogColumnName",
    "PresentationSubjectArea", "PresentationTableName", "PresentationColumnName",
    "BusinessLayerMappingInPresLayer", "PresLayerMappingInBusinessLayer", "BusinessModelName",
    "BusLayerLogicalTableName", "BusLayerLogicalColumnName", "PhysicalLayerMappingInBusinessLayer",
    "BusinessLayerMappingInPhysicalLayer", "PhysicalDbName", "PhysicalSchemaName",
    "PhysicalTableName", "PhysicalColumnName"];

# Cache of Neo4J CREATE NODE statements and the node IDs
nodeIdCache = dict();
relationIdCache = dict();

# Neo4J Node Types
reportNode = "OBIEEReport";
catalogColumnNode = "CatalogLayerColumn";
presentationColumnNode = "PresentationColumn";
businessColumnNode = "BusinessLayerLogicalColumn";
physicalColumnNode = "PhysicalColumn";

def connectToSqlServer():
    dbConnection = pyodbc.connect('Driver={SQL Server};'
                      'Server=E557LTRV;'
                      'Database=BIReportToEDWLineage;'
                      'Trusted_Connection=yes;')
    dbConnection.autocommit = False;
    dbCursor = dbConnection.cursor()
    return dbConnection, dbCursor;

escapeChars = {
    "\'": "\\'"
    };

def doCharEscapes(input):
    if not input:
        return input;
    input = input.strip();
    for key in escapeChars:
        if key in input:
            input = input.replace(key, escapeChars[key]);
    return input;

def executeSelectQuery(dbCursor, reportsInClause):
    stmt = fullSelectStmt.format(reportsInClause);
    dbCursor.execute(stmt)
    resultsDict = dict();
    for row in dbCursor:
        resultDict = dict();
        for field in enumerate(resultFieldsList):
            index = field[0];
            fieldName = field[1];
            fieldValue = row[index];
            resultDict[fieldName] = fieldValue;
        reportPathOriginal = resultDict["ReportPath"];
        reportPath = reportPathOriginal;
        if ("\\/" in reportPathOriginal):
            reportPath = reportPathOriginal.replace("\\/", "/");
        reportName = resultDict["ReportName"];
        reportFolder = reportPath[:-len(reportName)] if reportPath.endswith(reportName) else reportPath;
        resultDict["ReportFolder"] = doCharEscapes(reportFolder);
        resultDict["ReportName"] = doCharEscapes(reportName);
        resultsList = resultsDict.get(reportPath, list())
        resultsList.append(resultDict);
        resultsDict[reportPath] = resultsList;
    return resultsDict;

def getCypherSingleResponse(result, key):
    value = "";
    for line in result:
        value = line[key];
    return value;

def getNodeId(result):
    return getCypherSingleResponse(result, "nodeId");

def getRelationId(result):
    return getCypherSingleResponse(result, "relationId");

def nodeCreateStatement(nodeType, nodeProperties):
    # Create node statement like:
    #       CREATE (n:Person { name: 'Andy', title: 'Developer' }) RETURN n.id
    nodeCreateStmt = "CREATE (n:" + nodeType + " {";
    first = True;
    properties = "";
    for key in nodeProperties:
        val = nodeProperties[key];
        if (first):
            first = False;
        else:
            properties = properties + ", ";
        properties = properties + key + ": \'" + val + "\'";
    nodeCreateStmt = nodeCreateStmt + properties + "})  RETURN ID(n) as nodeId";
    return nodeCreateStmt;

def createNodeIfNotPresent(tx, nodeType, nodeProperties):
    nodeCreateStmt = nodeCreateStatement(nodeType, nodeProperties);
    if nodeCreateStmt in nodeIdCache:
        nodeId = nodeIdCache[nodeCreateStmt];
        print("\tNode with ID " + str(nodeId) + " already present for [" + nodeCreateStmt + "]");
        return nodeId;
    else:
        returnResult = tx.run(nodeCreateStmt);
        nodeId = getNodeId(returnResult);
        nodeIdCache[nodeCreateStmt] = nodeId;
        print("\tNew node created with ID " + str(nodeId) + " for [" + nodeCreateStmt + "]");
        return nodeId;

def nodeRelationCreateStatement(fromTuple, toTuple, relationName):
    # Create relationship between two nodes. Example:
    #       MATCH (n:OBIEEReport),(m:CatalogLayerColumn) WHERE ID(n) = 21 AND ID(m) = 58 
    #       CREATE (n)-[r:usesColumn]->(m);
    stmt = "MATCH (n:" + fromTuple[1] + "), (m:" + toTuple[1] + ") WHERE ID(n) = " + str(fromTuple[0]) + " AND ID(m) = " + str(toTuple[0]) + " CREATE (n)-[r:" + relationName + "]->(m) RETURN ID(r) as relationId";
    return stmt;

def createRelationIfNotPresent(tx, fromTuple, toTuple, relationName):
    relationCreateStmt = nodeRelationCreateStatement(fromTuple, toTuple, relationName);
    if relationCreateStmt in relationIdCache:
        relationId = relationIdCache[relationCreateStmt];
        print("\tRelation with ID " + str(relationId) + " already present for [" + relationCreateStmt + "]");
        return relationId;
    else:
        returnResult = tx.run(relationCreateStmt);
        relationId = getRelationId(returnResult);
        relationIdCache[relationCreateStmt] = relationId;
        print("\tNew relation created with ID " + str(relationId) + " for [" + relationCreateStmt + "]");
        return relationId;

def createReportNodeInNeo4j(tx, result):
    # Work on the OBIEEReport node
    reportName = result["ReportName"];
    folder = result["ReportFolder"];
    subjectArea = result["CatalogSubjectArea"];
    #createReportNodeStmt = nodeCreateStatement(reportNode, {"reportName": reportName, "folder": folder, "subjectArea": subjectArea});
    reportNodeId = createNodeIfNotPresent(tx, reportNode, {"reportName": reportName, "folder": folder, "subjectArea": subjectArea});
    return reportNodeId;

def createAndLinkCatalogLayerNodes(tx, result, reportNodeId):
    reportName = result["ReportName"];
    folder = result["ReportFolder"];
    reportPath = folder + reportName;
    tableName = result["CatalogTableName"];
    columnName = result["CatalogColumnName"];
    subjectArea = result["CatalogSubjectArea"];
    if (not tableName):
        print("\t\tCatalogLayerNodeCreate: tableName is empty for subjectArea[" + subjectArea + "].. cannot proceed");
        return None;
    if (not columnName):
        print("\t\tCatalogLayerNodeCreate: columnName is empty for subjectArea[" + subjectArea + "] tableName[" + tableName + "].. cannot proceed");
        return None;
    columnNodeId = createNodeIfNotPresent(tx, catalogColumnNode,
        {"columnName": columnName, "tableName": tableName, "subjectArea": subjectArea, "reportPath": reportPath});
    relationId = createRelationIfNotPresent(tx, (reportNodeId, reportNode), (columnNodeId, catalogColumnNode), "usesColumn");
    return columnNodeId;

def createAndLinkPresentationLayerNode(tx, result, catalogColumnNodeId):
    catSubjectArea = result["CatalogSubjectArea"];
    catTableName = result["CatalogTableName"];
    catColumnName = result["CatalogColumnName"];
    presentationSubjectArea = result["PresentationSubjectArea"];
    presentationTableName = result["PresentationTableName"];
    presentationColumnName = result["PresentationColumnName"];
    if (not presentationSubjectArea) or (not presentationTableName) or (not presentationColumnName):
        print("\t\tPresLayerNodeCreate: subjectArea/tableName/columnName is empty in Presentation Layer for Category Layer Entity [{0}.{1}.{2}].. cannot proceed".format(catSubjectArea, catTableName, catColumnName));
        return None;
    presentationColumnNodeId = createNodeIfNotPresent(tx, presentationColumnNode,
        {"presentationColumnName": presentationColumnName, "presentationTableName": presentationTableName, "presentationSubjectArea": presentationSubjectArea});
    relationId = createRelationIfNotPresent(tx, (catalogColumnNodeId, catalogColumnNode), (presentationColumnNodeId, presentationColumnNode), "mappedTo");
    return presentationColumnNodeId;

def createAndLinkBusinessLayerNode(tx, result, presLayerColumnNodeId):
    presentationSubjectArea = result["PresentationSubjectArea"];
    presentationTableName = result["PresentationTableName"];
    presentationColumnName = result["PresentationColumnName"];
    businessModelName = result["BusinessModelName"];
    businessLayerLogicalTableName = result["BusLayerLogicalTableName"];
    businessLayerLogicalColumnName = result["BusLayerLogicalColumnName"];
    if (not businessModelName) or (not businessLayerLogicalTableName) or (not businessLayerLogicalColumnName):
        print("\t\tBusinessLayerNodeCreate: businessModel/tableName/columnName is empty in Business Layer for Presentation Layer Entity [{0}.{1}.{2}].. cannot proceed".format(presentationSubjectArea, presentationTableName, presentationColumnName));
        return None;
    businessLayerColumnNodeId = createNodeIfNotPresent(tx, businessColumnNode,
        {"businessLayerLogicalColumnName": businessLayerLogicalColumnName, "businessLayerLogicalTableName": businessLayerLogicalTableName, "businessModelName": businessModelName});
    relationId = createRelationIfNotPresent(tx, (presLayerColumnNodeId, presentationColumnNode), (businessLayerColumnNodeId, businessColumnNode), "mappedTo");
    return businessLayerColumnNodeId;

def createAndLinkPhysicalLayerNode(tx, result, busLayerColumnNodeId):
    businessModelName = result["BusinessModelName"];
    businessLayerLogicalTableName = result["BusLayerLogicalTableName"];
    businessLayerLogicalColumnName = result["BusLayerLogicalColumnName"];
    physicalLayerMappingInBusinessLayer = result["PhysicalLayerMappingInBusinessLayer"];
    if (physicalLayerMappingInBusinessLayer == "NOT_AVAILABLE"):
        print("\t\tPhysicalLayerNodeCreate: Physical Layer Mapping is NOT defined in the Business Layer for Entity [{0}.{1}.{2}].. cannot proceed".format(businessModelName, businessLayerLogicalTableName, businessLayerLogicalColumnName));
        return None;
    physicalDbName = result["PhysicalDbName"];
    physicalSchemaName = result["PhysicalSchemaName"];
    physicalTableName = result["PhysicalTableName"];
    physicalColumnName = result["PhysicalColumnName"];
    if (not physicalDbName) or (not physicalTableName) or (not physicalColumnName):
        print("\t\tPhysicalLayerNodeCreate: databaseName/tableName/columnName is empty in Physical Layer for Business Layer Entity [{0}.{1}.{2}].. cannot proceed".format(businessModelName, businessLayerLogicalTableName, businessLayerLogicalColumnName));
        return None;
    if (not physicalSchemaName):
        physicalSchemaName = "None";
    physicalLayerColumnNodeId = createNodeIfNotPresent(tx, physicalColumnNode,
        {"physicalColumnName": physicalColumnName, "physicalTableName": physicalTableName, "physicalSchemaName": physicalSchemaName, "physicalDbName": physicalDbName});
    relationId = createRelationIfNotPresent(tx, (busLayerColumnNodeId, businessColumnNode), (physicalLayerColumnNodeId, physicalColumnNode), "mappedTo");
    return physicalLayerColumnNodeId;

def populateNeo4jDbForOneResult(tx, result):
    print("\tCreating the Report Node in Neo4J...");
    reportNodeId = createReportNodeInNeo4j(tx, result);
    print("\tCreating the Catalog Layer Node in Neo4J...");
    catalogColumnNodeId = createAndLinkCatalogLayerNodes(tx, result, reportNodeId);
    if (not catalogColumnNodeId):
        return;
    print("\tCreating the Presentation Layer Node in Neo4J...");
    presLayerColumnNodeId = createAndLinkPresentationLayerNode(tx, result, catalogColumnNodeId);
    if (not presLayerColumnNodeId):
        return;
    print("\tCreating the Business Layer Nodes in Neo4J...");
    busLayerColumnNodeId = createAndLinkBusinessLayerNode(tx, result, presLayerColumnNodeId);
    if (not busLayerColumnNodeId):
        return;
    print("\tCreating the Physical Layer Nodes in Neo4J...");
    physicalLayerColumnNodeId = createAndLinkPhysicalLayerNode(tx, result, busLayerColumnNodeId);

def populateNeo4jDb(dbCursor, neo4jSession, resultsList):
    tx = neo4jSession.begin_transaction();
    for result in resultsList:
        catSubjectArea = result["CatalogSubjectArea"];
        catTableName = result["CatalogTableName"];
        catColumnName = result["CatalogColumnName"];
        print("Processing the Catalog Column [{0}.{1}.{2}]...".format(catSubjectArea, catTableName, catColumnName));
        populateNeo4jDbForOneResult(tx, result);
    tx.commit();

def main():
    print("Connecting to Neo4J");
    neo4jURI = "bolt://localhost:7687"
    neo4jDriver = GraphDatabase.driver(neo4jURI, auth=("neo4j", "netapp"));
    print("Connected to Neo4J. Getting the session object");
    neo4jSession = neo4jDriver.session();
    print("Connected to Neo4J and got the session object. Now connecting to SQL Server");
    dbConnection, dbCursor = connectToSqlServer();
    #
    # Get the reports to process - either from the Dashboard 
    # or the ones mentioned in this script
    #
    if (processFullDashboardReports):
        reportPathsInClause = "SELECT DISTINCT [AnalysisPath] FROM [BIReportToEDWLineage].[dbo].[OBIEEDashboardReportsView]";
    else:
        reportPathList = reportsOfInterest;
        reportPathsInClause = ', '.join("'{0}'".format(path) for path in reportPathList);
    print("Fetching all of the 4 layers' data from SQL Server for all reports of interest...");
    resultsDict = executeSelectQuery(dbCursor, reportPathsInClause);

    for reportPath in resultsDict:
        print("  ");
        print("==============Processing the report: ", reportPath, "=================");
        resultsList = resultsDict[reportPath];
        populateNeo4jDb(dbCursor, neo4jSession, resultsList);
        #print("Report: ", reportPath);
        #resultsList = resultsDict[reportPath];
        #for result in resultsList:
        #    print("\t", result);
    print("Total nodes created:", len(nodeIdCache), "; Total relations created: ", len(relationIdCache));

if __name__== "__main__":
  main()

