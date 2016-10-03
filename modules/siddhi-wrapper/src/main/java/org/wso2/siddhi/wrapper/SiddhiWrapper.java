/*
* Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not
* use this file except in compliance with the License. You may obtain a copy
* of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for the
* specific language governing permissions and limitations under the License.
*/


package org.wso2.siddhi.wrapper;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.collection.queue.SiddhiQueue;
import org.wso2.siddhi.wrapper.util.SiddhiEventConsumer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class SiddhiWrapper {

    private String[] streamDefinitionArray;
    private String siddhiQuery;
    private SiddhiConfiguration siddhiConfiguration;
    private Map<String, SiddhiManager> dynamicSiddhiManagerImpl = new LinkedHashMap<String, SiddhiManager>();
    private List<SiddhiEventConsumer> siddhiEventConsumerList = new ArrayList<SiddhiEventConsumer>();
    public SiddhiBlockingQueue<SiddhiQueue<InEvent>> siddhiBlockingQueueGroup = new SiddhiBlockingQueue<SiddhiQueue<InEvent>>(60000);
    private SiddhiQueue<InEvent> currentProcessedEventQueue = new SiddhiQueue<InEvent>();
    private SiddhiQueue<InEvent> nextProcessedEventQueue = new SiddhiQueue<InEvent>();
    private Logger log = Logger.getLogger(SiddhiWrapper.class);
    private long lastEventTimeStamp;
    private long middleEventTimeStamp;
    private long withinTimeInterval = 6000;
    private AtomicInteger runningSiddhiManagerCount = new AtomicInteger(0);
    private int maxSiddhiManagerCount = 2;
    private int siddhiManagerCount = 0;
    private SiddhiWrapper siddhiWrapper;
    AtomicLong count = new AtomicLong(0);
    long start = 0;

    ///////////////////
    //    private Timer timer;
    //    ScheduledTask st;


    public void createExecutionPlan(String[] streamDefinitionArray, String siddhiQuery, SiddhiConfiguration siddhiConfiguration, int maxSiddhiManagerCount) {

        this.streamDefinitionArray = streamDefinitionArray;
        this.siddhiQuery = siddhiQuery;
        this.siddhiConfiguration = siddhiConfiguration;
        this.maxSiddhiManagerCount = maxSiddhiManagerCount;
        siddhiManagerCount++;
        spinSiddhiManagerInstance();
        siddhiWrapper = this;
    }

    public void registerCallback(SiddhiEventConsumer siddhiEventConsumer) {
        siddhiEventConsumerList.add(siddhiEventConsumer);
    }


    public void spinSiddhiManagerInstance() {
        SiddhiManager siddhiManager;

        if (siddhiConfiguration != null) {
            siddhiManager = new SiddhiManager(siddhiConfiguration);
        } else {
            siddhiManager = new SiddhiManager();
        }

        for (String streamDefinition : streamDefinitionArray) {
            siddhiManager.defineStream(streamDefinition);
        }

        //It is only a temporary hack.
        ///
        ////
        AddQueries.addPlayerStreams(siddhiManager);
        AddQueries.addBallStream(siddhiManager);
        AddQueries.addHitStream(siddhiManager);

        String queryReference = siddhiManager.addQuery(siddhiQuery);
        dynamicSiddhiManagerImpl.put(queryReference, siddhiManager);
        (new Thread(new EventConsumer(siddhiManager, queryReference))).start();
        Thread eventPublisher = new Thread(new EventPublisher(siddhiManager));
        eventPublisher.setPriority(Thread.MAX_PRIORITY);
        eventPublisher.start();
    }

    public void shutdown() {
        for (SiddhiManager siddhiManager : dynamicSiddhiManagerImpl.values()) {
            siddhiManager.shutdown();
        }
    }

    public void sentEvents(String streamId, Object[] event, long time) {

        long currentTimeInMillis = time;

        if (currentProcessedEventQueue.size() == 0) {
            lastEventTimeStamp = currentTimeInMillis + withinTimeInterval + 6000;
            middleEventTimeStamp = currentTimeInMillis + withinTimeInterval;
            currentProcessedEventQueue = nextProcessedEventQueue;
            nextProcessedEventQueue = new SiddhiQueue<InEvent>();
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
//            timer = new Timer();
//            st = new ScheduledTask();
//            timer.schedule(st, withinTimeInterval);


        } else if (currentTimeInMillis > middleEventTimeStamp && currentTimeInMillis < lastEventTimeStamp) {
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
            nextProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
        } else if (currentTimeInMillis > lastEventTimeStamp) {
            //timer.cancel();
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
            nextProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
            while (runningSiddhiManagerCount.get() == maxSiddhiManagerCount) {
                try {

                    synchronized (this) {
                        wait();
                    }
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            }

            siddhiBlockingQueueGroup.put(currentProcessedEventQueue);
            currentProcessedEventQueue = new SiddhiQueue<InEvent>();
            if (siddhiManagerCount < maxSiddhiManagerCount) {
                spinSiddhiManagerInstance();
                siddhiManagerCount++;
            }
        } else {
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
        }

    }


    class EventConsumer implements Runnable {

        SiddhiManager siddhiManager;
        String queryReference;

        EventConsumer(SiddhiManager siddhiManager, String queryReference) {
            this.siddhiManager = siddhiManager;
            this.queryReference = queryReference;
        }

        @Override
        public void run() {
            siddhiManager.addCallback(queryReference, new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    for (SiddhiEventConsumer siddhiEventConsumer : siddhiEventConsumerList) {
                        siddhiEventConsumer.receiveEvents(timeStamp, inEvents, removeEvents);

                    }
                }
            });
        }
    }

    class EventPublisher implements Runnable {

        SiddhiManager siddhiManager;
        InputHandler inputHandler;

        EventPublisher(SiddhiManager siddhiManager) {
            this.siddhiManager = siddhiManager;
            inputHandler = siddhiManager.getInputHandler("sensorStream");
        }

        @Override
        public void run() {
            while (true) {
                try{
                SiddhiQueue<InEvent> siddhiQueue = siddhiBlockingQueueGroup.poll();
                if (siddhiQueue != null) {
                    runningSiddhiManagerCount.incrementAndGet();
                    while (siddhiQueue.size() > 0) {
                        InEvent siddhiEvent = siddhiQueue.poll();
                        try {

                            inputHandler.send(siddhiEvent);
                            long countValue = count.incrementAndGet();

                            if (countValue == 1) {
                                start = System.currentTimeMillis();
                            }

                            if (countValue % 1000000 == 0) {
                                float percentageCompleted = (countValue / 49576080);
                                System.out.println("********** Events Completed : " + count + " Throughput : " + (countValue * 1000.0 / (System.currentTimeMillis() - start)) + " PercentageCompleted : " + percentageCompleted + "%");
                            }

                        } catch (Exception e) {
                            log.error("Error while sending events", e);
                        }

                    }
                    int activeSiddhiManager = runningSiddhiManagerCount.decrementAndGet();
                    if (activeSiddhiManager < 2) {
                        synchronized (siddhiWrapper) {
                            siddhiWrapper.notifyAll();
                        }
                    }

                }
                }catch (NullPointerException e){
                    System.out.println("NPE");
                }

            }
        }
    }


//    class ScheduledTask extends TimerTask {
//
//        public void run() {
//            if (currentProcessedEventQueue.size() > 0) {
//                siddhiBlockingQueueGroup.put(currentProcessedEventQueue);
//                currentProcessedEventQueue = new SiddhiQueue<InEvent>();
//                if (siddhiManagerCount < 3) {
//                    spinSiddhiManagerInstance();
//                    siddhiManagerCount++;
//                }
//            }
//
//            timer.cancel();
//        }
//    }


}
