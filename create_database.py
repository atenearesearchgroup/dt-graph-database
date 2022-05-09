from neo4j import GraphDatabase

"""
@Author: Paula Munoz - University of Malaga
"""


class BraccioData:

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        self.driver.close()

    """
    CREATION METHODS
    """

    def create_database(self, filename):
        with self.driver.session() as session:
            session.write_transaction(self._reset_database)
            session.write_transaction(self._import_data_from_csv, filename)
            session.write_transaction(self._create_next_relationships)

    @staticmethod
    def _import_data_from_csv(tx, filename):
        tx.run("LOAD CSV WITH HEADERS FROM \"file:///" + filename + "\" AS row "
                                                                    "MERGE (dt:DigitalTwin {name:row.digitalTwin}) "
                                                                    "MERGE (snp:Snapshot {s1:toFloatOrNull(row.s1), "
                                                                    "s2:toFloatOrNull(row.s2), "
                                                                    "s3:toFloatOrNull(row.s3), "
                                                                    "s4:toFloatOrNull(row.s4), "
                                                                    "s5:toFloatOrNull(row.s5), "
                                                                    "s6:toFloatOrNull(row.s6), "
                                                                    "isMoving:toFloatOrNull(row.isMoving)}) "
                                                                    "MERGE (ts:Timestamp "
                                                                    "{time:toFloatOrNull(row.timestamp)}) "
                                                                    "MERGE (dt)-[:IS_IN_STATE]->(snp) "
                                                                    "MERGE (snp)-[:AT_THE_TIME]->(ts)")

    @staticmethod
    def _create_next_relationships(tx):
        tx.run("MATCH (ts:Timestamp) "
               "WITH ts "
               "ORDER BY ts.time DESC "
               "WITH collect(ts) as timestamps "
               "FOREACH (i in range(0, size(timestamps) - 2) | "
               "FOREACH (ts1 in [timestamps[i]] | "
               "FOREACH (ts2 in [timestamps[i+1]] | "
               "CREATE (ts2)-[:NEXT]->(ts1))))")

    @staticmethod
    def _reset_database(tx):
        tx.run("MATCH (a) -[r] -> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")


if __name__ == "__main__":
    driver = BraccioData("neo4j://localhost:7687")
    driver.create_database("synthetic_data.csv")
    driver.close()
