import synthetic_data_generator
from graph_database.create_graph_database import GraphDatabaseDriver
from keyvalue_database.create_keyvalue_database import KeyValueDatabaseDriver
from db_driver import *
import csv_util
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mc
import colorsys


# To colour the graphic with a gradient
def scale_lightness(rgb, scale_l):
    h, l, s = colorsys.rgb_to_hls(*rgb)
    return colorsys.hls_to_rgb(h, min(1, l * scale_l), s=s)


# It resets the database and adds new data
def create_database(db: DatabaseDriver):
    db.reset_database()
    for case_study in case_studies:
        datafiles = csv_util.find_csv_files(input_files_path + case_study, ".csv")
        for file in datafiles:
            db.import_data_from_csv(input_files_path + case_study + file)

    db.close()


# Plots a bar graphic
def generate_graphic(keyvalue_performance: dict, graph_performance: dict):
    fig, ax = plt.subplots(figsize=(16, 9))

    ax.grid(color='slategray',
            linestyle='-.', linewidth=0.5,
            alpha=0.2)

    keys_list = list(keyvalue_performance.keys())
    index = np.arange(len(keyvalue_performance[keys_list[0]].keys()))
    bar_width = 1 / len(index) / 3

    # Keyvalue db
    color = mc.ColorConverter.to_rgb("red")
    redis = [scale_lightness(color, scale) for scale in np.linspace(1.7, 0.8, len(keyvalue_performance))]
    for j in range(len(keyvalue_performance)):
        plt.bar(index + j * bar_width,
                [keyvalue_performance[keys_list[j]][key] for key in keyvalue_performance[keys_list[0]].keys()],
                bar_width,
                color=redis[j],
                label=f"Redis {keys_list[j]}")

    # Graph db
    color = mc.ColorConverter.to_rgb("blue")
    neo = [scale_lightness(color, scale) for scale in np.linspace(1.7, 0.8, len(keyvalue_performance))]
    for j in range(len(graph_performance)):
        plt.bar(index + (j + len(keys_list)) * bar_width,
                [graph_performance[keys_list[j]][key] for key in graph_performance[keys_list[0]].keys()],
                bar_width,
                color=neo[j],
                label=f"Neo4j {keys_list[j]}")

    # plt.xlabel('Queries', fontsize=15)
    plt.ylabel('Execution time (s)', fontsize=15)
    plt.title('Execution time by Query', fontsize=20)
    plt.xticks(index + bar_width * (len(keys_list) - 1 / 2),
               ['Q%d' % (x + 1) for x in range(len(keyvalue_performance[keys_list[0]].keys()))], fontsize=15)
    plt.legend(fontsize=12)

    plt.show()


if __name__ == "__main__":
    input_files_path = "./datafiles/input/"
    case_studies = ["braccio/", "nxj-car/"]

    number_of_cases = 5
    incremental_factor = 2

    keyvalue_driver = KeyValueDatabaseDriver("localhost", "6379")
    graph_driver = GraphDatabaseDriver("neo4j://localhost:7687")

    performance_keyvalue = {}
    performance_graph = {}
    for i in range(1, number_of_cases + 1):
        factor = i * incremental_factor

        synthetic_data_generator.reset_case_studies(input_files_path, case_studies)
        synthetic_data_generator.generate_all_case_studies(input_files_path, case_studies, factor, factor)

        create_database(keyvalue_driver)
        performance_keyvalue[str(factor)] = keyvalue_driver.execute_queries()
        print(performance_keyvalue)

        create_database(graph_driver)
        performance_graph[str(factor)] = graph_driver.execute_queries()
        print(performance_graph)

    generate_graphic(performance_keyvalue, performance_graph)
