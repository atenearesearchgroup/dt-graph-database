#!/bin/bash

# TEMPLATE: Fill the <__> with your specific data
# Copy the synthetic data file into the docker container and execute the python script to create the database
sudo docker cp "<your_local_path>/sinthetic_data.csv" <container_id>:/var/lib/neo4j/import
python3 "<your_local_path>/create_database.py"