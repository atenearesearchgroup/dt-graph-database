# dt-graph-database
In this repository, we include a basic example of setting up a graph database for storing Digital Twins' execution traces. Additionally, we compare this approach with one using Redis, a key-value database. We are using a Braccio robotic arm and a Lego Mindstorms NXJ as examples.
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

### Key-value database - redis

#### Docker installation

1. Pull an image from Docker Hub:
```
docker pull redis:latest
```

2. Run a Docker image (either pulled or locally generated):
```
sudo docker run --name redis-timeless -d --restart always --publish=6379:6379 redis:latest 
```

#### Perform the analysis
Run main.py, and it will start the execution and generate a graphic with the corresponding results.
