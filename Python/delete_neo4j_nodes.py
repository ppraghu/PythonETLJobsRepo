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
    url = "https://ec2-52-32-225-35.us-west-2.compute.amazonaws.com:9000/"
    driver = GraphDatabase.driver(url, auth=("neo4j", "xxyyzz"));
    session = driver.session();
    for node_label in nodes_list:
        print("Deleting node(s) with label " + node_label);
        delete_stmt = "MATCH (n:" + node_label + ") DETACH DELETE n";
        print("\tExecuting [" + delete_stmt + "]");
        session.run(delete_stmt);

if __name__== "__main__":
    main()

