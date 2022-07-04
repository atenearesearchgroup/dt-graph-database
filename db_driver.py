import random
from abc import ABC, abstractmethod
import time

"""
@Author: Paula Munoz - University of Malaga
"""


class DatabaseDriver(ABC):
    """
    MANAGEMENT METHODS
    """

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def reset_database(self):
        pass

    @abstractmethod
    def initialize_database(self, filename):
        pass

    @abstractmethod
    def import_data_from_csv(self, filename):
        pass

    """
    QUERIES
    """

    @abstractmethod
    def car_follow_the_line(self, parameters: dict):
        pass

    @abstractmethod
    def cars_in_squared_area(self, parameters: dict):
        pass

    @abstractmethod
    def cars_collided(self, parameters: dict):
        pass

    @abstractmethod
    def arm_servo_avg_angle(self, parameters: dict):
        pass

    @abstractmethod
    def arm_servo_aggregated_information(self, parameters: dict):
        pass

    def execute_queries(self):
        time_result = {}
        queries = {"follow the line": self.car_follow_the_line,
                   "avg angle servo": self.arm_servo_avg_angle,
                   "squared area": self.cars_in_squared_area,
                   # "collisions": self.cars_collided,
                   "avg time window servo": self.arm_servo_aggregated_information}

        parameters = self._get_prepared_parameters()

        for keys in queries.keys():
            start_time = time.time()
            queries[keys](parameters)
            time_result[keys] = time.time() - start_time
        return time_result

    @staticmethod
    def _get_rand_parameters():
        cars = ["NXJCar", "NXJCarPT", "XXXCar", "XXXCarPT", "SSSCar", "SSSCarPT"]
        braccios = ["braccio", "braccio2"]

        timestamp_car = 16274847350

        timestamp_braccio = 0
        random.seed(333)
        parameters = {"twin_id": random.choice(cars),
                      "ts1_car": timestamp_car + random.randint(0, 20),
                      "ts1_braccio": timestamp_braccio + random.randint(0, 10),
                      "sq_xa": random.uniform(0, 6),
                      "sq_ya": random.uniform(0, 6),
                      "sq_length": random.uniform(0, 6),
                      "servo": random.randint(1, 7),
                      "time_window": random.randint(2, 10),
                      "collision_distance": random.uniform(0, 2)
                      }

        parameters["ts2_car"] = parameters["ts1_car"] + 10 * random.randint(2, 20)
        parameters["ts2_braccio"] = parameters["ts1_braccio"] + random.randint(2, 20)

        return parameters

    @staticmethod
    def _get_prepared_parameters():
        parameters = {"twin_id": "NXJCar",
                      "ts1_car": 16274847350,
                      "ts2_car": 16274847380,
                      "ts1_braccio": 0,
                      "ts2_braccio": 20,
                      "sq_xa": 3,
                      "sq_ya": 0,
                      "sq_length": 3,
                      "servo": 3,
                      "time_window": 10,
                      "collision_distance": 3
                      }

        return parameters
