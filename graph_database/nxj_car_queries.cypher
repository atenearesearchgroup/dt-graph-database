// Snapshots for any Digital Twin, which occur during three consecutive timestamps between two given
MATCH (before:Timestamp)
WHERE before.value >= 16274847350
MATCH (after:Timestamp)
WHERE after.value <= 16274847540
AND before.value < after.value
MATCH (before)-[:NEXT]->(after)
MATCH (dt:DigitalTwin)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(before)
MATCH (dt2:DigitalTwin)-[:IS_IN_STATE]->(snp2)-[:AT_THE_TIME]->(after)
RETURN dt, snp, dt2, snp2, before, after
LIMIT 3

// Given a position x, find how many cars were in that position in a given time interval
MATCH (snp:Snapshot)
MATCH (ts:Timestamp)
WHERE ts.value <= 16274847540 AND ts.value >= 16274847350
AND snp.xPos >= 13 AND snp.xPos < 14
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
WITH DISTINCT dt
RETURN count(dt)

// This one gives which cars
MATCH (snp:Snapshot)
MATCH (ts:Timestamp)
WHERE ts.value <= 16274847380 AND ts.value >= 16274847350
AND snp.xPos >= 3 AND snp.xPos <= 6
AND snp.yPos >= 0 AND snp.yPos <= 3
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
RETURN dt

// Given a square (x1..x2,y1..y2), find how many cars were in that square in a given time interval
MATCH (snp:Snapshot)
MATCH (ts:Timestamp)
WHERE ts.value <= 16274847380 AND ts.value >= 16274847350
AND snp.xPos >= 3 AND snp.xPos <= 6
AND snp.yPos >= 0 AND snp.yPos <= 3
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
WITH DISTINCT dt
RETURN count(dt)

// This one gives which cars
MATCH (snp:Snapshot)
MATCH (ts:Timestamp)
WHERE ts.value <= 16274847380 AND ts.value >= 16274847350
AND snp.xPos >= 3 AND snp.xPos < 17
AND snp.yPos >= 3 AND snp.yPos < 17
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
RETURN dt, snp, ts

// Find collisions between cars in a time interval
MATCH (snp : Snapshot)
WITH *, point({x: snp.xPos, y: snp.yPos, crs: 'cartesian'}) AS p1
MATCH (snp2 : Snapshot)
WITH *, point({x: snp2.xPos, y: snp2.yPos, crs: 'cartesian'}) AS p2
WITH snp, snp2, point.distance(p1,p2) AS dist
WHERE dist < 2
MATCH (dt : DigitalTwin)
MATCH (dt)-[:IS_IN_STATE]->(snp)
MATCH (dt2 : DigitalTwin)
MATCH (dt2)-[:IS_IN_STATE]->(snp2)
WHERE dt <> dt2
MATCH (snp)-[:AT_THE_TIME]->(ts)<-[:AT_THE_TIME]-(snp2)
RETURN dt, dt2, snp, snp2, ts LIMIT 3

// Find the average value of the light sensor in a time interval
MATCH (dt: DigitalTwin {name:'NXJCar'})
MATCH (snp: Snapshot)
MATCH (ts: Timestamp)
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
WHERE ts.value >= 16274847350
AND ts.value <= 16274847490
RETURN avg(snp.light)