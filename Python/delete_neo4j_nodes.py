from neo4j import GraphDatabase

nodesList = [
"OBIEEReport",
"CatalogLayerColumn",
"PresentationColumn",
"BusinessLayerLogicalColumn",
"PhysicalColumn",
"DbSynonym"
];

def main():
    neo4jURI = "bolt://localhost:7687"
    driver = GraphDatabase.driver(neo4jURI, auth=("neo4j", "netapp"));
    session = driver.session();
    for nodeLabel in nodesList:
        print("Deleting node(s) with label " + nodeLabel);
        #deleteStmt = "MATCH (n:QuoteSalesRepSplit {quoteNumber: "12345"}) DETACH DELETE n"
        deleteStmt = "MATCH (n:" + nodeLabel + ") DETACH DELETE n";
        print("\tExecuting [" + deleteStmt + "]");
        session.run(deleteStmt);

if __name__== "__main__":
    main()

