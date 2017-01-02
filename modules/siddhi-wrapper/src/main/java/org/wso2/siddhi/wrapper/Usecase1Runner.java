package org.wso2.siddhi.wrapper;


import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.wrapper.extensions.NearCheckExecuter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Usecase1Runner {

    public static void main(String[] args) {

        SiddhiConfiguration configuration = new SiddhiConfiguration();
        List<Class> siddhiExtensions = new ArrayList<Class>();
        siddhiExtensions.add(NearCheckExecuter.class);
        configuration.setSiddhiExtensions(siddhiExtensions);

        SiddhiManager siddhiManager = new SiddhiManager(configuration);

        String streamDef = "define stream sensorStream ( sid string, ts long, " + "x double, y double,  z double, "
                           + "v double, a double, vx double, vy double, vz double, ax double, ay double, az double, tsr long, tsms long )";

        InputHandler sensorStreamInputHandler = siddhiManager.defineStream(streamDef);
        AddQueries.addPlayerStreams(siddhiManager);
        AddQueries.addBallStream(siddhiManager);
        AddQueries.addHitStream(siddhiManager);

        String patternQuery = "from every h1 = hitStream -> h2 = hitStream[h1.pid != pid and h1.tid == tid] -> h3 = hitStream[h1.pid == pid]  \n" +
                              " within 2 seconds\n" +
                              " select h1.pid as player1, h2.pid as player2, h3.pid as player3, h1.tsr as tStamp , h2.tsr as tStamp1 , h3.tsr as tStamp2 \n" +
                              " insert into patternMatchedStream;";


        String queryReference = siddhiManager.addQuery(patternQuery);
        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
            }
        });


        try {
            BenchMarkRunner.sendEvents("/home/mohan/myfiles/debbs/full-game", sensorStreamInputHandler);
            //BenchMarkRunner.sendEvents("/home/cep/pattern-perf-test/full-game", sensorStreamInputHandler);
        } catch (IOException e) {
            System.out.println("Exception when reading the event file : " + e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }


    }


}
