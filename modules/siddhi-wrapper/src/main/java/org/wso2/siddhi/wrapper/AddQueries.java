package org.wso2.siddhi.wrapper;

import org.wso2.siddhi.core.SiddhiManager;

public class AddQueries {

    public static void addPlayerStreams(SiddhiManager siddhiManager) {
        String outputStreamName = "sensorIdentifiedStream";

        if (siddhiManager.getStreamDefinition(outputStreamName) != null) {
            System.out.println("Stream " + outputStreamName + " already defined!");
        } else {
            String q0A =
                    "from  sensorStream [ sid == '13' or sid == '14' or sid == '97' or sid == '98'] " +
                    "select 'Nick Gertje' as pid, 'A' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms  " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q0A);

            String q01 =
                    "from  sensorStream [ sid == '47' or sid == '16'] " +
                    "select 'Dennis Dotterweich' as pid, 'A' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q01);

            String q02 =
                    "from  sensorStream [ sid == '49' or sid == '88'] " +
                    "select 'Niklas Waelzlein' as pid, 'A' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q02);

            String q03 =
                    "from  sensorStream [ sid == '19' or sid == '52'] " +
                    "select 'Wili Sommer' as pid, 'A' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q03);

            String q04 =
                    "from  sensorStream [ sid == '53' or sid == '54'] " +
                    "select 'Philipp Harlass' as pid , 'A' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q04);

            String q05 =
                    "from  sensorStream [ sid == '23' or sid == '24'] " +
                    "select 'Roman Hartleb' as pid, 'A' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q05);

            String q06 =
                    "from  sensorStream [ sid == '57' or sid == '58'] " +
                    "select 'Erik Engelhardt' as pid, 'A' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q06);

            String q07 =
                    "from  sensorStream [ sid == '59' or sid == '28'] " +
                    "select 'Sandro Schneider' as pid ,'A' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q07);

            // -----------team b------------------------

            String q0B =
                    "from  sensorStream [ sid == '61' or sid == '62' or sid == '99' or sid == '100'] " +
                    "select 'Leon Krapf' as pid,'B' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q0B);

            String q08 =
                    "from  sensorStream [ sid == '63' or sid == '64'] " +
                    "select 'Kevin Baer' as pid,'B' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q08);

            String q09 =
                    "from  sensorStream [ sid == '65' or sid == '66'] " +
                    "select 'Luca Ziegler' as pid,'B' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q09);

            String q010 =
                    "from  sensorStream [ sid == '67' or sid == '68'] " +
                    "select 'Ben Mueller' as pid ,'B' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q010);

            String q011 =
                    "from  sensorStream [ sid == '69' or sid == '38'] " +
                    "select 'Vale Reitstetter' as pid,'B' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q011);

            String q012 =
                    "from  sensorStream [ sid == '71' or sid == '40'] " +
                    "select 'Christopher Lee' as pid ,'B' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q012);

            String q013 =
                    "from  sensorStream [ sid == '73' or sid == '74'] " +
                    "select 'Leon Heinze' as pid ,'B' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q013);

            String q014 =
                    "from  sensorStream [ sid == '75' or sid == '44'] " +
                    "select 'Leo Langhans' as pid ,'B' as tid, ts, x, y, z, v, a, vx, vy, vz, ax, ay, az, sid, tsms " +
                    "insert into " + outputStreamName + ";";
            siddhiManager.addQuery(q014);

        }
    }

    public static void addBallStream(SiddhiManager siddhiManager) {

        if (siddhiManager.getStreamDefinition("ballStream") != null) {
            System.out.println("Stream ballStream already defined!");
        } else {

            String q211 = "from  sensorStream [ sid == '4' or sid == '8' or sid == '10' or sid =='12'] " +
                          "select sid, ts, x, y, z, a, v, ax, ay, az, vx, vy, vz, tsr, 'ball' as id  " +
                          "insert into ballStream;";
            siddhiManager.addQuery(q211);

            //calculate ball ins
            String q22 = "from  ballStream [ x < 52483 and x > 0 and y > -33960 and y < 33965] " +
                         "insert into ballInStream ;";
            siddhiManager.addQuery(q22);

        }
    }

    public static void addHitStream(SiddhiManager siddhiManager) {

        if (siddhiManager.getStreamDefinition("hitStream") != null) {
            System.out.println("Stream hitStream already defined!");
        } else {

            //calculate ballNearPlayerStream
            String q23 = "from  ballInStream#window.length(1) as b join sensorIdentifiedStream#window.length(1)  as p unidirectional " +
                         "on debs:getDistance(b.x,b.y,b.z, p.x, p.y, p.z) < 1000 and b.a > 55 " +
                         "select p.sid, p.ts, p.x, p.y, p.z, p.v, p.vx ,p.vy, p.vz, p.a, p.ax, p.ay, p.az, p.pid,p.tid, b.sid as ball_sid  " +
                         "insert into hitStream;";
            siddhiManager.addQuery(q23);


        }
    }

    public static void addBallLeavingStream(SiddhiManager siddhiManager) {

        if (siddhiManager.getStreamDefinition("ballLeavingStream") != null) {
            System.out.println("Stream ballLeavingStream already defined!");
        } else {

            String q25 = "from b1 = ballStream[ x < 52483 and x > 0 and y > -33960 and y < 33965], b2 = ballStream[ b1.sid==sid and (x > 52483 or x < 0 or y < -33960 or y > 33965)] " +
                         "select b2.sid, b2.ts, b2.x, b2.y, b2.z, b2.a, b2.id  " +
                         "insert into ballLeavingStream ";
            siddhiManager.addQuery(q25);


        }
    }

}
