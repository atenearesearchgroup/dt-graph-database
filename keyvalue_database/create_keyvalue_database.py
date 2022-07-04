import math

import redis
from csv_util import *
from db_driver import *
import re

"""
@Author: Paula Munoz - University of Malaga
"""


class KeyValueDatabaseDriver(DatabaseDriver):
    """
    MANAGEMENT METHODS
    """

    def __init__(self, host, port):
        self.driver = redis.Redis(
            host=host,
            port=port)

    def close(self):
        self.driver.close()

    def reset_database(self):
        self.driver.flushdb()

    def initialize_database(self, filename):
        self.reset_database()
        self.import_data_from_csv(filename)

    def import_data_from_csv(self, filename):
        reader = list(get_reader(filename, ",")[1])
        headers = reader[0]

        for line in reader[1:]:
            twin_type = line[headers.index("twinType")]
            twin_id = line[headers.index("twinId")]
            execution_id = line[headers.index("executionId")]
            timestamp = line[headers.index("timestamp")]
            # Build the hash
            hash_values = {
                "twinId": twin_id,
                "executionId": execution_id,
                "timestamp": timestamp,
            }

            # Save snapshot to the Data Lake
            key = twin_type + f"OutputSnapshot:{twin_id}:{execution_id}:{timestamp}"

            for i in range(4, len(headers)):
                value = line[i]
                hash_values[f"{headers[i]}"] = value
                if re.match(r'^-?\d+(\.\d+)?$', value):
                    self.driver.zadd(f"{twin_type}OutputSnapshot:{twin_id}:{execution_id}_{headers[i].upper()}_LIST",
                                     {key: float(value)})

            self.driver.hset(key, mapping=hash_values)
            self.driver.sadd(twin_type + "_LIST", twin_id)
            self.driver.zadd(twin_type + "OutputSnapshot_PROCESSED", {key: 0})
            self.driver.sadd(twin_type + "OutputSnapshot:" + twin_id + "_EXECUTIONID_LIST", execution_id)
            # print(f"Saved output object: {key}")

            # Save a reference to this hash in the twin's "history"
            history_key = twin_type + f"OutputSnapshot:{twin_id}:{execution_id}_HISTORY"
            self.driver.zadd(history_key, {key: timestamp})

    """
    QUERIES
    """

    def car_follow_the_line(self, parameters: dict):
        # Retrieve and store reusable parameters to improve performance
        ts1 = parameters['ts1_car']
        ts2 = parameters['ts2_car']
        twin_id = parameters['twin_id']
        twin_type = 'PT' if 'PT' in twin_id else 'DT'

        # Build the basic compound key to query on Redis
        base_key = f"{twin_type}OutputSnapshot:{twin_id}"

        # Get all execution ids in which our twin is involved
        execution_ids = self.driver.smembers(f"{base_key}_EXECUTIONID_LIST")

        # To calculate the average
        sum_result = 0
        number_elements = 0
        for execution_id in execution_ids:
            execution_id = execution_id.decode()
            # Get all the keys between the established timestamps
            history_query_key = f"{base_key}:{execution_id}_HISTORY"
            timestamps = self.driver.zrangebyscore(history_query_key, ts1, ts2)
            number_elements += len(timestamps)

            # Get all the light values
            for ts in timestamps:
                ts = ts.decode()
                sum_result += float(self.driver.hgetall(ts)['light'.encode()])

        # To avoid division between 0
        return 0 if number_elements == 0 else sum_result / number_elements

    def cars_in_squared_area(self, parameters: dict):
        result = []

        # Retrieve and store reusable parameters to improve performance
        ts1 = parameters['ts1_car']
        ts2 = parameters['ts2_car']
        sq_xa = parameters['sq_xa']
        sq_ya = parameters['sq_ya']
        sq_length = parameters['sq_length']

        existing_twin_types = ['DT', 'PT']
        # Query for all twins
        for twin_type in existing_twin_types:
            twins = self.driver.smembers(f"{twin_type}_LIST")
            for twin_id in twins:
                # Build the basic compound key to query on Redis
                base_key = f"{twin_type}OutputSnapshot:{twin_id.decode()}"

                # Get all execution ids in which our twin is involved
                execution_ids = self.driver.smembers(f"{base_key}_EXECUTIONID_LIST")
                for execution_id in execution_ids:
                    # Get all the keys between the established timestamps
                    history_query_key = f"{base_key}:{execution_id.decode()}_HISTORY"
                    timestamps = [el.decode() for el in self.driver.zrangebyscore(history_query_key, ts1, ts2)]

                    # Get all the keys with the xPos established
                    x_pos_query_key = f"{base_key}:{execution_id.decode()}_XPOS_LIST"
                    x_pos = [el.decode() for el in self.driver.zrangebyscore(x_pos_query_key, sq_xa, sq_xa + sq_length)]

                    # Get all the keys with the yPos established
                    y_pos_query_key = f"{base_key}:{execution_id.decode()}_YPOS_LIST"
                    y_pos = [el.decode() for el in self.driver.zrangebyscore(y_pos_query_key, sq_ya, sq_ya + sq_length)]

                    result.extend(set(timestamps).intersection(set(x_pos).intersection(y_pos)))

        # Returns the name of the different cars
        return set([el[:el.index(':1')] for el in result])

    def cars_collided(self, parameters: dict):
        # Retrieve and store reusable parameters to improve performance
        collision_distance = parameters['collision_distance']

        # Query for all digital twins
        collisions = []
        twins = list(self.driver.smembers("DT_LIST"))
        for i in range(0, len(twins)):
            for j in range(i + 1, len(twins)):
                # Build the basic compound key to query on Redis
                twin_a_base_key = f"DTOutputSnapshot:{twins[i].decode()}"
                twin_b_base_key = f"DTOutputSnapshot:{twins[j].decode()}"

                # Get all execution ids in which our twin is involved
                execution_ids_a = self.driver.smembers(f"{twin_a_base_key}_EXECUTIONID_LIST")
                execution_ids_b = self.driver.smembers(f"{twin_b_base_key}_EXECUTIONID_LIST")
                execution_ids = list(execution_ids_a.intersection(execution_ids_b))

                for execution_id in execution_ids:
                    execution_id = execution_id.decode()
                    # Get all the keys with the xPos established
                    twin_a_ts_query = f"{twin_a_base_key}:{execution_id}_HISTORY"
                    a_ts = [el[1] for el in self.driver.zrange(twin_a_ts_query, 0, -1, withscores=True)]

                    # Get all the keys with the yPos established
                    twin_b_ts_query = f"{twin_b_base_key}:{execution_id}_HISTORY"
                    b_ts = [el[1] for el in self.driver.zrange(twin_b_ts_query, 0, -1, withscores=True)]

                    common_ts = set(a_ts).intersection(b_ts)
                    for ts in common_ts:
                        x_pos = "xPos".encode()
                        a_twin_hash = self.driver.hgetall(f"{twin_a_base_key}:{execution_id}:{int(ts)}")
                        b_twin_hash = self.driver.hgetall(f"{twin_b_base_key}:{execution_id}:{int(ts)}")
                        if x_pos in a_twin_hash and x_pos in b_twin_hash:
                            a_point = (
                                float(a_twin_hash[x_pos].decode()), float(a_twin_hash[('yPos').encode()].decode()))
                            b_point = (
                                float(b_twin_hash[x_pos].decode()), float(b_twin_hash[('yPos').encode()].decode()))

                            dist = math.dist(a_point, b_point)
                            if dist < collision_distance:
                                collisions.append(f"{twin_a_base_key}:{twin_b_base_key}:{execution_id}:{ts}")

        return collisions

    def arm_servo_avg_angle(self, parameters: dict):
        # Retrieve and store reusable parameters to improve performance
        ts1 = parameters['ts1_braccio']
        ts2 = parameters['ts2_braccio']
        num_servo = parameters['servo']

        twins = list(self.driver.smembers("DT_LIST"))

        # To calculate the average
        sum_result = 0
        number_elements = 0
        for twin_id in twins:
            # Build the basic compound key to query on Redis
            base_key = f"DTOutputSnapshot:{twin_id.decode()}"

            # Get all execution ids in which our twin is involved
            execution_ids = self.driver.smembers(f"{base_key}_EXECUTIONID_LIST")

            for execution_id in execution_ids:
                execution_id = execution_id.decode()
                # Get all the keys between the established timestamps
                history_query_key = f"{base_key}:{execution_id}_HISTORY"
                timestamps = self.driver.zrangebyscore(history_query_key, ts1, ts2)
                number_elements += len(timestamps)

                # Get all the light values
                for ts in timestamps:
                    ts = ts.decode()
                    sum_result += float(self.driver.hgetall(ts)[('currentAngles_' + str(num_servo)).encode()])

        # To avoid division between 0
        return 0 if number_elements == 0 else sum_result / number_elements

    def arm_servo_aggregated_information(self, parameters: dict):
        result = []
        # Retrieve and store reusable parameters to improve performance
        time_window = parameters['time_window']
        num_servo = parameters['servo']

        twins = filter(lambda c: "braccio".encode() in c, list(self.driver.smembers("DT_LIST")))
        for twin in twins:

            # Build the basic compound key to query on Redis
            twin_base_key = f"DTOutputSnapshot:{twin.decode()}"

            # Get all execution ids in which our twin is involved
            execution_ids = self.driver.smembers(f"{twin_base_key}_EXECUTIONID_LIST")
            current_angles = ("currentAngles_" + str(num_servo)).encode()

            for execution_id in execution_ids:
                execution_id = execution_id.decode()
                # Get all the keys with the xPos established
                twin_a_ts_query = f"{twin_base_key}:{execution_id}_HISTORY"
                a_ts = [el[1] for el in self.driver.zrange(twin_a_ts_query, 0, -1, withscores=True)]

                for i in range(0, len(a_ts)):
                    # To calculate the average
                    twin_hash = self.driver.hgetall(f"{twin_base_key}:{execution_id}:{int(a_ts[i])}")
                    sum_result = float(twin_hash[current_angles].decode())
                    number_elements = 1
                    for j in range(i + 1, len(a_ts)):
                        twin_hash = self.driver.hgetall(f"{twin_base_key}:{execution_id}:{int(a_ts[j])}")
                        if current_angles in twin_hash:
                            sum_result += float(twin_hash[current_angles].decode())
                            number_elements += 1
                        if a_ts[j] - a_ts[i] == time_window:
                            if number_elements == 0:
                                avg = 0
                            else:
                                avg = sum_result / number_elements
                            result.append(f"{twin_base_key}:{a_ts[i]}:{a_ts[j]}:{avg}")
                            break
                        elif a_ts[j] - a_ts[i] > time_window:
                            break

        return result
