from lxml import etree

mappingErrorsList = [];
DASH_AND_SPACE = "- ";

def getObjectName(tree):
    names = tree.xpath('/REPOSITORY_OBJECT/OBJECT_NAME/text()');
    return names[0];

def putQuotesAroundComma(value):
    if ("," in value):
        value = "\"" + value + "\"";
    return value;
def getCatRefTableName(tableName):
    if (tableName.startswith(DASH_AND_SPACE)):
        return tableName.replace(DASH_AND_SPACE, "", 1);
    else:
        return tableName;

def processTableNode(subjectAreaName, tableNodeTree):
    tableName = tableNodeTree.xpath('/REPOSITORY_OBJECT/OBJECT_NAME/text()')[0];
    columns = tableNodeTree.xpath('/REPOSITORY_OBJECT/REPOSITORY_OBJECT[OBJECT_TYPE[text() = \'Presentation Column\']]');
    if (len(columns) == 0):
        mappingErrorsList.append("[" + subjectAreaName + "].[" + tableName + "] does not have any Presentation Columns");
    else:
        subjectAreaName = putQuotesAroundComma(subjectAreaName);
        tableName = putQuotesAroundComma(tableName);
        catalogTableRef = getCatRefTableName(tableName);
        for column in columns:
            columnTree = etree.ElementTree(column);
            presColumnName = putQuotesAroundComma(getObjectName(columnTree));
            logicalColumnName = putQuotesAroundComma(columnTree.xpath('/REPOSITORY_OBJECT/LOGICAL_COLUMN/text()')[0]);
            print(subjectAreaName + "," + tableName + "," + catalogTableRef + "," + presColumnName + "," + logicalColumnName);

tree = etree.parse(r'Presentation.xml');
subjectAreaNodes = tree.xpath('/LAYER/REPOSITORY_OBJECT[OBJECT_TYPE[text() = \'Subject Area\']]');
print("SubjectArea,TableName,CatalogTableRef,ColumnName,BusinessLayerMapping");
for subjectArea in subjectAreaNodes:
    subjectAreaTree = etree.ElementTree(subjectArea);
    subjectAreaName = getObjectName(subjectAreaTree);
    subNodes = subjectAreaTree.xpath('/REPOSITORY_OBJECT/REPOSITORY_OBJECT');
    for subNode in subNodes:
        subNodeTree = etree.ElementTree(subNode);
        subNodeTypes = subNodeTree.xpath('/REPOSITORY_OBJECT/OBJECT_TYPE/text()');
        subNodeType = subNodeTypes[0];
        subNodeName = getObjectName(subNodeTree);
        if (subNodeType == 'Presentation Table'):
            processTableNode(subjectAreaName, subNodeTree);

mappingErrorsFile = open("PresentationLayerColumnMappingErrors.txt", "w");
for err in mappingErrorsList:
    mappingErrorsFile.write(err + "\n");
mappingErrorsFile.close();
