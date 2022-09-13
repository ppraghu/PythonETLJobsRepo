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
    neo4jURI = "http://localhost:7687"
    #user_name = "neo4j"
    #password = "xxyyzz"
    #driver = GraphDatabase.driver(neo4jURI, auth=(user_name, password));
    driver = GraphDatabase.driver(neo4jURI, auth=("neo4j", "xxyyzz"));
    session = driver.session();
    for node_label in nodes_list:
        print("Deleting node(s) with label " + node_label);
        delete_stmt = "MATCH (n:" + node_label + ") DETACH DELETE n";
        print("\tExecuting [" + delete_stmt + "]");
        session.run(delete_stmt);

if __name__== "__main__":
    main()

