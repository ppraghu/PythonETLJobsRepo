from lxml import etree

SCHEMA_NA = "SCHEMA_NOT_AVAILABLE";

def processTableNode(dbName, schemaName, tableNodeTree):
    tableName = tableNodeTree.xpath('/REPOSITORY_OBJECT/OBJECT_NAME/text()')[0];
    columnNames = tableNodeTree.xpath('/REPOSITORY_OBJECT/REPOSITORY_OBJECT[OBJECT_TYPE[text() = \'Physical Column\']]/OBJECT_NAME/text()');
    if (schemaName == SCHEMA_NA):
        schemaName = ""; 
    for columnName in columnNames:
        # Format: "EDWNI_CORE".""."OBIEE_APP"."CALENDAR_PRETI_TRNSCTN_CREATE_DATE"."FISCAL_WEEK_START_DATE"
        businessLayerMappingStr = "\"" + dbName + "\".\"\".\"" + schemaName + "\".\"" + tableName + "\".\"" + columnName + "\"";
        print(dbName + "," + schemaName + "," + tableName + "," + columnName + "," + businessLayerMappingStr);
        #print("					<OBJECT_NAME>{0}</OBJECT_NAME>".format(columnName));

def processSchemaNode(dbName, schemaNodeTree):
    schemaName = schemaNodeTree.xpath('/REPOSITORY_OBJECT/OBJECT_NAME/text()')[0];
    tableNodes = schemaNodeTree.xpath('/REPOSITORY_OBJECT/REPOSITORY_OBJECT[OBJECT_TYPE[text() = \'Physical Table\']]');
    for tableNode in tableNodes:
        tableNodeTree = etree.ElementTree(tableNode);
        processTableNode(dbName, schemaName, tableNodeTree);

tree = etree.parse(r'Physical.xml');
databaseNodes = tree.xpath('/LAYER/REPOSITORY_OBJECT[OBJECT_TYPE[text() = \'Database\']]');
print("DatabaseName,SchemaName,TableName,ColumnName,BusinessLayerMapping");
for db in databaseNodes:
    #print (etree.tostring(db));
    dbTree = etree.ElementTree(db);
    dbNames = dbTree.xpath('/REPOSITORY_OBJECT/OBJECT_NAME/text()');
    dbName = dbNames[0];
    subNodes = dbTree.xpath('/REPOSITORY_OBJECT/REPOSITORY_OBJECT');
    for subNode in subNodes:
        subNodeTree = etree.ElementTree(subNode);
        subNodeTypes = subNodeTree.xpath('/REPOSITORY_OBJECT/OBJECT_TYPE/text()');
        subNodeType = subNodeTypes[0];
        if (subNodeType == 'Physical Schema'):
            processSchemaNode(dbName, subNodeTree);
        elif (subNodeType == 'Physical Catalog'): # Treat this same as Physical Schema
            processSchemaNode(dbName, subNodeTree);
        elif (subNodeType == 'Physical Table'):
            processTableNode(dbName, SCHEMA_NA, subNodeTree);

