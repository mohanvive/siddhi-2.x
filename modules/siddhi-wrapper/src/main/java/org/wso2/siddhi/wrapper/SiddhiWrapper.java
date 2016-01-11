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

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;


public class SiddhiWrapper {

    private String[] streamDefinitionArray;
    private String siddhiQuery;
    private SiddhiConfiguration siddhiConfiguration;
    private Map<String, SiddhiManager> dynamicSiddhiManagerImpl = new LinkedHashMap<String, SiddhiManager>();
    private List<SiddhiEventConsumer> siddhiEventConsumerList = new ArrayList<SiddhiEventConsumer>();
    private Logger log = Logger.getLogger(SiddhiWrapper.class);
    public SiddhiQueue<SiddhiQueue<InEvent>> siddhiEventQueueGroup = new SiddhiQueue<SiddhiQueue<InEvent>>();
    private SiddhiQueue<InEvent> currentProcessedEventQueue = new SiddhiQueue<InEvent>();
    private long lastEventTimeStamp;
    private long withinTimeInterval = 10000;
    private Timer timer;
    ScheduledTask st;
    int totalQueues = 0;
    AtomicLong count = new AtomicLong(0);
    long start = 0;
    int siddhiMangerCount = 0;

    public void createExecutionPlan(String[] streamDefinitionArray, String siddhiQuery, SiddhiConfiguration siddhiConfiguration) {

        this.streamDefinitionArray = streamDefinitionArray;
        this.siddhiQuery = siddhiQuery;
        this.siddhiConfiguration = siddhiConfiguration;
        spinSiddhiManagerInstance();
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

    public void sentEvents(String streamId, Object[] event) {

        if (currentProcessedEventQueue.size() == 0) {
            long currentTimeInMillis = System.currentTimeMillis();
            lastEventTimeStamp = currentTimeInMillis + withinTimeInterval;
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
//            timer = new Timer();
//            st = new ScheduledTask();
//            timer.schedule(st, withinTimeInterval);


        } else if (System.currentTimeMillis() > lastEventTimeStamp) {
            //timer.cancel();
            siddhiEventQueueGroup.put(currentProcessedEventQueue);
            currentProcessedEventQueue = new SiddhiQueue<InEvent>();
            long currentTimeInMillis = System.currentTimeMillis();
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
            totalQueues++;
            if ((totalQueues % 3) == 0 && (siddhiMangerCount < 15)) {
                System.out.println("spin in main " + siddhiEventQueueGroup.size());
                spinSiddhiManagerInstance();
                siddhiMangerCount++;
            }
        } else {
            long currentTimeInMillis = System.currentTimeMillis();
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
                SiddhiQueue<InEvent> siddhiQueue = siddhiEventQueueGroup.poll();
                if (siddhiQueue != null) {
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
                }
            }
        }
    }

    class ScheduledTask extends TimerTask {

        public void run() {
            if (currentProcessedEventQueue.size() > 0) {
                siddhiEventQueueGroup.put(currentProcessedEventQueue);
                currentProcessedEventQueue = new SiddhiQueue<InEvent>();
                totalQueues++;
                if ((totalQueues % 3) == 0 && (siddhiMangerCount < 20)) {
                    //System.out.println("spin in schedule " + siddhiEventQueueGroup.size());
                    spinSiddhiManagerInstance();
                    siddhiMangerCount++;
                }
                //System.out.println("entered & removed 2*****");
            }

            timer.cancel();
        }
    }


}
