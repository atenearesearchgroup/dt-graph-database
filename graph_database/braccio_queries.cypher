// Create a sample DT, with two snapshots
CREATE (braccio:DigitalTwin:RobotArm {name:'braccio'}),
    (tmsp1:Timestamp {value:123456}),
    (tmsp2:Timestamp {value:123457}),
    (snp1:Snapshot {s1:'0', s2:'0', s3:'0', s4:'0', s5:'0', s6:'0', moving:'false'}),
    (snp1)-[:AT_THE_TIME]->(tmsp1),
    (snp1)-[:AT_THE_TIME]->(tmsp2),
    (braccio)-[:IS_IN_STATE]->(snp1),
    (tmsp1)-[:NEXT]->(tmsp2)

// Create the next relationship between consecutive timestamps
MATCH (ts:Timestamp)
WITH ts
ORDER BY ts.value DESC
WITH collect(ts) as timestamps
FOREACH (i in range(0, size(timestamps) - 2) |
  FOREACH (ts1 in [timestamps[i]] |
  FOREACH (ts2 in [timestamps[i+1]] |
  CREATE (ts1)-[:NEXT]->(ts2))))

// Calculate the average value of the arm Servo s3 in the time interval between t1 (0) and t2 (20)
MATCH (dt:DigitalTwin {name:'braccio'})
MATCH (ts:Timestamp)
WHERE ts.value <= 20 AND ts.value >= 0
MATCH (dt:DigitalTwin)
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
WITH collect(snp.currentAngles_3) as snapshots
UNWIND snapshots as s
RETURN avg(s)

// Calculate the average value of the arm Servo s3 for all RobotArms in the System
// in the time interval between t1 (0) and t2 (20)
MATCH (ts:Timestamp)
WHERE ts.value <= 20 AND ts.value >= 0
MATCH (dt:DigitalTwin)
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
WITH collect(snp.currentAngles_3) as snapshots
UNWIND snapshots as s
RETURN avg(s)

// Calculate the average value of the servo s3 for each of the robot arms in the System
MATCH (ts:Timestamp)
WHERE ts.value <= 20 AND ts.value >= 0
MATCH (dt:DigitalTwin)
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
WITH dt, snp.currentAngles_3 as snapshots
RETURN dt, avg(snapshots)

// Time series aggregation
MATCH (dt:DigitalTwin)-[:IS_IN_STATE]->()-[:AT_THE_TIME]->(upperBound:Timestamp)
WHERE dt.name STARTS WITH 'braccio'
MATCH (dt)-[:IS_IN_STATE]->()-[:AT_THE_TIME]->(lowerBound:Timestamp)
MATCH timePeriod=(lowerBound)-[:NEXT*1..20]->(upperBound)
WHERE upperBound.value - lowerBound.value = 10
WITH *, nodes(timePeriod) as timestamps
MATCH (dt)-[:IS_IN_STATE]->(snp:Snapshot)-[:AT_THE_TIME]->(ts:Timestamp)
WHERE ts in timestamps
WITH upperBound, lowerBound, timestamps, dt, collect(snp) as snapshots
UNWIND snapshots as s
RETURN dt, upperBound, lowerBound, avg(s.currentAngles_3)

