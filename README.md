# dt-graph-database
In this repository, we include a basic example on how to set up a graph database for storing Digital Twins' execution traces. We are using a Braccio robotic arm as an example.

## Installation
### Graph database - neo4j

Download a new instance of [neo4j](https://neo4j.com/). It is recommended to use a Docker container. 

#### Docker installation

1. Pull an image from Docker Hub:
```
docker pull neo4j:latest
```

2. Run a Docker image (either pulled or locally generated):
```
sudo docker run --name neo4j-database -d --restart always --publish=7474:7474 --publish=7687:7687 --volume=$HOME/neo4j/data:/data --env NEO4J_ACCEPT_LICENSE_AGREEMENT=yes --env NEO4J_AUTH=none neo4j:latest 
```

3. Get the container id for the next step:
```
docker ps
```

#### Create and fill your database with synthetic data
1. Fill the file [inject_data_template.sh](https://github.com/atenearesearchgroup/dt-graph-database/blob/main/inject_data_template.sh) with your local path to the corresponding files and the docker container id from the database.
2. Execute [inject_data_template.sh](https://github.com/atenearesearchgroup/dt-graph-database/blob/main/inject_data_template.sh)

#### Check your database
You can access your container on [localhost:7474](http://localhost:7474/). Select _No authentication_ as the access mode.