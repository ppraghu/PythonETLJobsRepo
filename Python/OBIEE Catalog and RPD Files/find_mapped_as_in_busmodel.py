from lxml import etree

def getObjectName(tree):
    names = tree.xpath('/REPOSITORY_OBJECT/OBJECT_NAME/text()');
    return names[0];

def getObjectType(tree):
    names = tree.xpath('/REPOSITORY_OBJECT/OBJECT_TYPE/text()');
    return names[0];

def printParentNodes(model, count):
    parent = model.getparent();
    path = [];
    while parent is not None:
        name = parent.tag;
        #print("Parent: ", parent.tag);
        if (parent.tag == 'REPOSITORY_OBJECT'):
            nodeTree = etree.ElementTree(parent);
            #objName = getObjectName(nodeTree);
            objType = getObjectType(nodeTree);
            name = name + "[" + objType + "]";
        path.append(name);
        parent = parent.getparent();
    fullPath = ' '.join([str(elem + ", ") for elem in path])
    print("Count: ", count, "\t - ", fullPath);

def printColumnNames(model, count):
    parent = model.getparent();
    itemTree = etree.ElementTree(parent);
    columnName = itemTree.xpath('/ITEM/COLUMN/text()')[0];
    print(columnName);


tree = etree.parse(r'BusinessModelAndMapping.xml');
mappedAsNodes = tree.xpath('//MAPPED_AS');
count = 1;
for model in mappedAsNodes:
    #printParentNodes(model, count);
    printColumnNames(model, count);
    count = count + 1;
