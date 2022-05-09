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
ORDER BY ts.time DESC
WITH collect(ts) as timestamps
FOREACH (i in range(0, size(timestamps) - 2) |
  FOREACH (ts1 in [timestamps[i]] |
  FOREACH (ts2 in [timestamps[i+1]] |
  CREATE (ts1)-[:NEXT]->(ts2))))

// Calculate the average value of the arm Servo s3 in the time interval between t1 (0) and t2 (20)
MATCH (dt:DigitalTwin {name:'braccio'})
MATCH (ts:Timestamp)
WHERE ts.time <= 20 AND ts.time >= 0
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
WITH collect(snp.s3) as snapshots
UNWIND snapshots as s
RETURN avg(s)

// Calculate the average value of the arm Servo s3 for all RobotArms in the System
// in the time interval between t1 (0) and t2 (20)
MATCH (dt:RobotArm)
MATCH (ts:Timestamp)
WHERE ts.time = 40
MATCH (dt)-[:IS_IN_STATE]->(snp)-[:AT_THE_TIME]->(ts)
WITH collect(snp.s3) as snapshots
UNWIND snapshots as s
RETURN avg(s)


