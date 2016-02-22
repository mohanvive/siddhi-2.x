package org.wso2.siddhi.wrapper;


import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.wrapper.extensions.NearCheckExecuter;
import org.wso2.siddhi.wrapper.util.SiddhiEventConsumer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Usecase2Runner {

    public static void main(String[] args) {

        int siddhiCount = Integer.parseInt(args[0]);

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();
        List<Class> siddhiExtensions = new ArrayList<Class>();
        siddhiExtensions.add(NearCheckExecuter.class);
        siddhiConfiguration.setSiddhiExtensions(siddhiExtensions);

        String streamDef[] = new String[]{"define stream sensorStream ( sid string, ts long, " + "x double, y double,  z double, "
                + "v double, a double, vx double, vy double, vz double, ax double, ay double, az double, tsr long, tsms long )"};

        String patternQuery = "from every  (h1 = hitStream -> h2 = hitStream[h1.pid != pid]) -> h3 = hitStream[h1.pid == pid] \n" +
                " within 10 seconds\n" +
                " select h1.pid as player1, h2.pid as player2\n" +
                " insert into patternMatchedStream;";


        SiddhiWrapper siddhiWrapper = new SiddhiWrapper();
        siddhiWrapper.createExecutionPlan(streamDef, patternQuery, siddhiConfiguration, siddhiCount);
        siddhiWrapper.registerCallback(new SiddhiEventConsumer() {
            @Override
            public void receiveEvents(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                //EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });


        try {
            sendEvents("/myfiles/debbs/full-game", siddhiWrapper);
        } catch (IOException e) {
            System.out.println("Exception when reading the event file : " + e);
        } catch (InterruptedException e) {
            System.out.println(e);
        } catch (Exception e){
            System.out.println(e);
        }

        siddhiWrapper.shutdown();
        System.out.println("Ended");

    }


    public static void sendEvents(String filename, SiddhiWrapper siddhiWrapper) throws IOException, InterruptedException {

        try {
            BufferedReader br = new BufferedReader(new FileReader(filename), 10 * 1024 * 1024);

            long count = 0;
            String line = br.readLine();
            long start = System.currentTimeMillis();

            while (line != null) {
                String[] dataStr = line.split(",");
                line = br.readLine();

                double v_kmh = Double.valueOf(dataStr[5]) * 60 * 60 / 1000000000;
                double a_ms = Double.valueOf(dataStr[6]) / 1000000;

                long time = Long.valueOf(dataStr[1]);


                if ((time >= 10753295594424116l && time <= 12557295594424116l) || (time >= 13086639146403495l && time <= 14879639146403495l)) {
                    Object[] data = new Object[]{dataStr[0], time, Double.valueOf(dataStr[2]),
                            Double.valueOf(dataStr[3]), Double.valueOf(dataStr[4]), v_kmh,
                            a_ms, Integer.valueOf(dataStr[7]), Integer.valueOf(dataStr[8]),
                            Integer.valueOf(dataStr[9]), Integer.valueOf(dataStr[10]), Integer.valueOf(dataStr[11]), Integer.valueOf(dataStr[12]),
                            System.nanoTime(), ((Double) (time * Math.pow(10, -9))).longValue()};

                    siddhiWrapper.sentEvents("sensorStream", data);
                    count++;

                    if (count % 1000000 == 0) {
                        float percentageCompleted = (count / 49576080);
                        System.out.println("Events Completed : " + count + " Throughput : " + (count * 1000.0 / (System.currentTimeMillis() - start)) + " PercentageCompleted : " + percentageCompleted + "%");
                    }
                }
            }


            long currentTime = System.currentTimeMillis();
            System.out.println("Processing took " + (currentTime - start) + " ms and throughput = " + (1000 * count / ((currentTime - start))));
            System.out.println("***** Processing took " + (currentTime - siddhiWrapper.start) + " ms and throughput = " + (1000 * siddhiWrapper.count.get() / ((currentTime - siddhiWrapper.start))));

        } catch (Exception e) {
            System.out.println(e);
        }

        System.out.println("Queue size " + siddhiWrapper.siddhiBlockingQueueGroup.size());
    }


}
