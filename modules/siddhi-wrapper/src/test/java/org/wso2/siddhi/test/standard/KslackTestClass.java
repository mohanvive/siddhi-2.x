package org.wso2.siddhi.test.standard;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

/**
 * Created with IntelliJ IDEA.
 * User: mohan
 * Date: 3/6/16
 * Time: 6:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class KslackTestClass {

//    public static void main(String[] args) {
//
////        SiddhiQueue<InEvent> currentProcessedEventQueue = new SiddhiQueue<InEvent>();
////        currentProcessedEventQueue.put(new InEvent("fdfd",232323l , new Object[]{"ds"}));
////        SiddhiQueue<InEvent> nextQueue = currentProcessedEventQueue;
////        currentProcessedEventQueue = new SiddhiQueue<InEvent>();
////
////        System.out.println(currentProcessedEventQueue.size());
////        System.out.println(nextQueue.size());
//
//
////        HashSet<String> al = new HashSet<String>();
////        al.add("Ravi");
////        al.add("Vijay");
////        al.add("Ravi");
////        al.add("Ajay");
////
////        System.out.println(al.size());
////
////        HashSet<Event> al = new HashSet<Event>();
////        al.add(new InEvent("id",67677, new Object[]{"ii","22"}));
////        al.add(new InEvent("id",67677, new Object[]{"ii","22"}));
////
////        System.out.println(al.size());
//
//
//
//
//
//
//
//
//
//    }

    static final Logger log = Logger.getLogger(KslackTestClass.class);
    private int count;
    private long value;
    private boolean eventArrived;
    long startingTime = 0;
    long endingTime = 0;

    @Before
    public void init() {
        count = 0;
        value = 0;
        eventArrived = false;
        startingTime = 0;
        endingTime = 0;
    }


    @Test
    public void perfTest() throws InterruptedException {
        log.info("Window PerfTest");
        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.defineStream("define stream inputStream (eventtt long, price long, volume long); ");

        String queryReference = siddhiManager.addQuery("from inputStream#window.kslack(eventtt) select eventtt, price, volume " +
                " insert into outputStream; ");

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (org.wso2.siddhi.core.event.Event event : inEvents) {
                    count++;

                    if (count == 1) {
                        org.junit.Assert.assertEquals(1l, event.getData()[0]);
                    }

                    if (count == 2) {
                        org.junit.Assert.assertEquals(4l, event.getData()[0]);
                    }

                    if (count == 3) {
                        org.junit.Assert.assertEquals(3l, event.getData()[0]);
                    }

                    if (count == 4) {
                        org.junit.Assert.assertEquals(5l, event.getData()[0]);
                    }

                    if (count == 5) {
                        org.junit.Assert.assertEquals(6l, event.getData()[0]);
                    }

                    if (count == 6) {
                        org.junit.Assert.assertEquals(7l, event.getData()[0]);
                    }

                    if (count == 7) {
                        org.junit.Assert.assertEquals(8l, event.getData()[0]);
                    }

                    if (count == 8) {
                        org.junit.Assert.assertEquals(9l, event.getData()[0]);
                    }

                    if (count == 9) {
                        org.junit.Assert.assertEquals(10l, event.getData()[0]);
                    }

                    if (count == 10) {
                        org.junit.Assert.assertEquals(13l, event.getData()[0]);
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiManager.getInputHandler("inputStream");

        inputHandler.send(new Object[]{1l, 700f, 100l});
        inputHandler.send(new Object[]{4l, 60.5f, 200l});
        inputHandler.send(new Object[]{3l, 60.5f, 200l});
        inputHandler.send(new Object[]{5l, 700f, 100l});
        inputHandler.send(new Object[]{6l, 60.5f, 200l});
        inputHandler.send(new Object[]{9l, 60.5f, 200l});
        inputHandler.send(new Object[]{7l, 700f, 100l});
        inputHandler.send(new Object[]{8l, 60.5f, 200l});
        inputHandler.send(new Object[]{10l, 60.5f, 200l});
        inputHandler.send(new Object[]{13l, 60.5f, 200l});

        Thread.sleep(2000);
        org.junit.Assert.assertEquals("Event count", 9, count);
        siddhiManager.shutdown();
    }


}
