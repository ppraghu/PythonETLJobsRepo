from neo4j import GraphDatabase

nodes_list = [
"OBIEEReport",
"CatalogLayerColumn",
"PresentationColumn",
"BusinessLayerLogicalColumn",
"PhysicalColumn",
"DbSynonym"
];

def main():
    neo4j_uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", "netapp"));
    session = driver.session();
    for node_label in nodes_list:
        print("Deleting node(s) with label " + node_label);
        delete_stmt = "MATCH (n:" + node_label + ") DETACH DELETE n";
        print("\tExecuting [" + delete_stmt + "]");
        session.run(delete_stmt);

if __name__== "__main__":
    main()

