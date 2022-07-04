from neo4j import GraphDatabase
from csv_util import *
from db_driver import *
import re

"""
@Author: Paula Munoz - University of Malaga
"""


class GraphDatabaseDriver(DatabaseDriver):

    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)

    """
    MANAGEMENT METHODS
    """

    def close(self):
        self.driver.close()

    def reset_database(self):
        self.driver.session().write_transaction(self._reset_database)

    def initialize_database(self, filename):
        self.reset_database()
        self.import_data_from_csv(filename)

    def import_data_from_csv(self, filename):
        with self.driver.session() as session:
            session.write_transaction(self._import_data_from_csv_file, filename)
            session.write_transaction(self._create_next_relationships)

    @staticmethod
    def _reset_database(tx):
        tx.run("MATCH (a) -[r] -> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")

    @staticmethod
    def _import_data_from_csv_file(tx, filename):
        pt = "PT"
        physical_twin = "PhysicalTwin"
        digital_twin = "DigitalTwin"
        reader = list(get_reader(filename, ",")[1])
        headers = reader[0]

        for line in reader[1:]:
            command = "MERGE "
            twin_type = line[headers.index("twinType")]
            twin_id = line[headers.index("twinId")]
            execution_id = line[headers.index("executionId")]
            timestamp = line[headers.index("timestamp")]

            command += f"({twin_id}:{digital_twin if twin_type != pt else physical_twin} {{name: '{twin_id}'}})\n"
            command += "MERGE "
            command += f"(t:Timestamp {{value: {timestamp}}})\n"

            attributes = f"executionId:'{execution_id}', "
            for i in range(4, len(headers)):
                value = line[i]
                value = f"'{value}'" if re.match(r'^-?\d+(\.\d+)?$', value) is None else value
                attributes += f"{headers[i]}:{value}"
                attributes += ", " if i + 1 < len(headers) else ""
            command += f"MERGE (s:Snapshot {{{attributes}}})\n"

            command += "MERGE (s)-[:AT_THE_TIME]->(t)\n"
            command += f"MERGE ({twin_id})-[:IS_IN_STATE]->(s)"

            # print(command)
            tx.run(command)

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
               "MERGE (ts1)-[:NEXT]->(ts2))))")

    """
    QUERIES
    """

    def car_follow_the_line(self, parameters):
        self.execute_graph_query(self._car_follow_the_line, parameters)

    @staticmethod
    def _car_follow_the_line(tx, parameters):
        result = tx.run(f"MATCH (dt: DigitalTwin {{name:'{parameters['twin_id']}'}})"
                        "MATCH (snp: Snapshot) "
                        "MATCH (ts: Timestamp) "
                        "MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts) "
                        f"WHERE ts.value >= {parameters['ts1_car']} "
                        f"AND ts.value <= {parameters['ts2_car']} "
                        "RETURN avg(snp.light)")
        return result.single()[0]

    def cars_in_squared_area(self, parameters):
        self.execute_graph_query(self._cars_in_squared_area, parameters)

    @staticmethod
    def _cars_in_squared_area(tx, parameters):
        result = tx.run("MATCH (snp:Snapshot)"
                        "MATCH (ts:Timestamp) "
                        f"WHERE ts.value >= {parameters['ts1_car']} AND ts.value <= {parameters['ts2_car']} "
                        f"AND snp.xPos >= {parameters['sq_xa']} AND snp.xPos <= {parameters['sq_xa'] + parameters['sq_length']} "
                        f"AND snp.yPos >= {parameters['sq_ya']} AND snp.yPos <= {parameters['sq_ya'] + parameters['sq_length']} "
                        "MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts) "
                        "WITH DISTINCT dt "
                        "RETURN dt")
        return result.values()

    def cars_collided(self, parameters):
        self.execute_graph_query(self._cars_collided, parameters)

    @staticmethod
    def _cars_collided(tx, parameters):
        result = tx.run("MATCH (snp : Snapshot) "
                        "WITH *, point({x: snp.xPos, y: snp.yPos, crs: 'cartesian'}) AS p1 "
                        "MATCH (snp2 : Snapshot) "
                        "WITH *, point({x: snp2.xPos, y: snp2.yPos, crs: 'cartesian'}) AS p2 "
                        "WITH snp, snp2, point.distance(p1,p2) AS dist "
                        f"WHERE dist < {parameters['collision_distance']} "
                        "MATCH (dt : DigitalTwin) "
                        "MATCH (dt)-[:IS_IN_STATE]->(snp) "
                        "MATCH (dt2 : DigitalTwin) "
                        "MATCH (dt2)-[:IS_IN_STATE]->(snp2) "
                        "WHERE dt <> dt2 "
                        "MATCH (snp)-[:AT_THE_TIME]->(ts)<-[:AT_THE_TIME]-(snp2) "
                        "RETURN DISTINCT(ts), dt, dt2, snp, snp2")
        return result.values()

    def arm_servo_avg_angle(self, parameters):
        self.execute_graph_query(self._arm_servo_avg_angle, parameters)

    @staticmethod
    def _arm_servo_avg_angle(tx, parameters):
        result = tx.run("MATCH (ts:Timestamp) "
                        f"WHERE ts.value <= {parameters['ts2_braccio']} AND ts.value >= {parameters['ts1_braccio']} "
                        "MATCH (dt:DigitalTwin) "
                        "MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts) "
                        f"WITH collect(snp.currentAngles_{parameters['servo']}) as snapshots "
                        "UNWIND snapshots as s "
                        "RETURN avg(s)")
        return result.single()[0]

    def arm_servo_aggregated_information(self, parameters):
        self.execute_graph_query(self._arm_servo_aggregated_information, parameters)

    @staticmethod
    def _arm_servo_aggregated_information(tx, parameters):
        result = tx.run("MATCH (dt:DigitalTwin)-[:IS_IN_STATE]->()-[:AT_THE_TIME]->(upperBound:Timestamp) "
                        "WHERE dt.name STARTS WITH 'braccio' "
                        "MATCH (dt)-[:IS_IN_STATE]->()-[:AT_THE_TIME]->(lowerBound:Timestamp) "
                        "MATCH timePeriod=(lowerBound)-[:NEXT*1..20]->(upperBound) "
                        f"WHERE upperBound.value - lowerBound.value = {parameters['time_window']} "
                        "WITH *, nodes(timePeriod) as timestamps "
                        "MATCH (dt)-[:IS_IN_STATE]->(snp:Snapshot)-[:AT_THE_TIME]->(ts:Timestamp) "
                        "WHERE ts in timestamps "
                        "WITH upperBound, lowerBound, timestamps, dt, collect(snp) as snapshots "
                        "UNWIND snapshots as s "
                        f"RETURN dt.name as dtname, upperBound, lowerBound, avg(s.currentAngles_{parameters['servo']}) as av")
        return [f"{v.values('dtname')}:{v.values('upperBound')}:{v.values('lowerBound')}:{v.values('av')}\n" for v in
                result]

    def execute_graph_query(self, method, parameters):
        with self.driver.session() as session:
            result = session.read_transaction(method, parameters)
        return result
