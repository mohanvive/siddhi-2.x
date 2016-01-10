package org.wso2.siddhi.wrapper;


import org.wso2.siddhi.core.stream.input.InputHandler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BenchMarkRunner {

    public static void sendEvents(String filename, InputHandler inputHandler) throws IOException, InterruptedException {

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

                    inputHandler.send(data);
                    count++;

                    if (count % 1000000 == 0) {
                        float percentageCompleted = (count / 49576080);
                        System.out.println("Events Completed : " + count + " Throughput : " + (count * 1000.0 / (System.currentTimeMillis() - start)) + " PercentageCompleted : " + percentageCompleted + "%");
                    }
                }
            }


            long currentTime = System.currentTimeMillis();
            System.out.println("Processing took " + (currentTime - start) + " ms and throughput = " + (1000 * count / ((currentTime - start))));
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}




