package org.wso2.siddhi.wrapper;


import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.wrapper.extensions.NearCheckExecuter;
import org.wso2.siddhi.wrapper.util.SiddhiEventConsumer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UsecaseTester {

    static InputHandler reorderEventInputHandler;

    public static void main(String[] args) {

        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.defineStream("define stream patternMatchedStream (player1 string, player2 string, player3 string, tStamp long, tStamp1 long, tStamp2 long); ");
        String queryReference = siddhiManager.addQuery("from patternMatchedStream#window.kslack(100, 2000) select *  " +
                                                       " insert into filteredOutputStream; ");

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(inEvents);
            }
        });

        reorderEventInputHandler = siddhiManager.getInputHandler("patternMatchedStream");

        try {
            reorderEventInputHandler.send(132385423175683l, new Object[]{"Leo Langhans", "Ben Mueller", "Leo Langhans", 123848655051059l, 123848655133167l, 123848655198130l});
            Thread.sleep(5000);
            reorderEventInputHandler.send(132385424992197l, new Object[]{"Leo Langharrrrns", "Ben Mueller", "Leo Langhans", 123848655051059l, 123848655133167l, 123848655198130l});
            System.out.println("TT");
            reorderEventInputHandler.send(132385423175683l, new Object[]{"Leo Langhans", "Ben Mueller", "Leo Langhans", 123848655051059l, 123848655133167l, 123848655198130l});
            reorderEventInputHandler.send(132385429269160l, new Object[]{"Leo Langhaddns", "Ben Mueller", "Leo Langhans", 123848655051059l, 123848655133167l, 123848655198130l});
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        try {
            Thread.sleep(80000);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }


    }


}
