from lxml import etree

noMappingsList = [];

def getObjectName(tree):
    names = tree.xpath('/REPOSITORY_OBJECT/OBJECT_NAME/text()');
    return names[0];

def processTableNode(modelName, tableNodeTree):
    tableName = tableNodeTree.xpath('/REPOSITORY_OBJECT/OBJECT_NAME/text()')[0];
    columnNames = tableNodeTree.xpath('/REPOSITORY_OBJECT/REPOSITORY_OBJECT[OBJECT_TYPE[text() = \'Logical Column\']]/OBJECT_NAME/text()');
    for columnName in columnNames:
        # Format: "AMAZON BOOKINGS"."AWS_USERS"."ACTIVE_USER"
        presLayerMappingStr = "\"" + modelName + "\".\"" + tableName + "\".\"" + columnName + "\"";
        #print(modelName + "," + tableName + "," + columnName + "," + presLayerMappingStr);
        # Find the corresponding MAPPED_AS field
        xpQuery = '/REPOSITORY_OBJECT/REPOSITORY_OBJECT/MAPPING/ITEM[COLUMN[text() = \'{0}\']]/MAPPED_AS/text()'.format(columnName);
        #print(xpQuery);
        mappedAsValue = tableNodeTree.xpath(xpQuery);
        if (len(mappedAsValue) == 0):
            noMappingsList.append(modelName + "." + tableName + "." + columnName);
            mappedAsValueTxt = "NOT_AVAILABLE";
        else:
            mappedAsValueTxt = mappedAsValue[0];
            # Double-quote the string if it already contains the , character
            if ("," in mappedAsValueTxt):
                mappedAsValueTxt = "\"" + mappedAsValueTxt + "\"";
        print(modelName + "," + tableName + "," + columnName + "," + presLayerMappingStr + "," + mappedAsValueTxt);

tree = etree.parse(r'BusinessModelAndMapping.xml');
businessModelNodes = tree.xpath('/LAYER/REPOSITORY_OBJECT[OBJECT_TYPE[text() = \'Business Model\']]');
print("BusinessModelName,TableName,ColumnName,PresentationLayerMapping,PhysicalLayerMapping");
for model in businessModelNodes:
    busModelTree = etree.ElementTree(model);
    businessModelName = getObjectName(busModelTree);
    subNodes = busModelTree.xpath('/REPOSITORY_OBJECT/REPOSITORY_OBJECT');
    for subNode in subNodes:
        subNodeTree = etree.ElementTree(subNode);
        subNodeTypes = subNodeTree.xpath('/REPOSITORY_OBJECT/OBJECT_TYPE/text()');
        subNodeType = subNodeTypes[0];
        subNodeName = getObjectName(subNodeTree);
        if (subNodeType == 'Logical Table'):
            processTableNode(businessModelName, subNodeTree);

noMappingsListFile = open("BusinessLayerColumnsNoMapping.txt", "w");
for col in noMappingsList:
    noMappingsListFile.write("[" + col + "]: No MAPPED_AS found.\n");
noMappingsListFile.close();