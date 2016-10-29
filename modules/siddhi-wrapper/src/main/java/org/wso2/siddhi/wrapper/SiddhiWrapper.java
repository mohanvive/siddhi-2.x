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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class SiddhiWrapper {

    public SiddhiBlockingQueue<SiddhiQueue<InEvent>> siddhiBlockingQueueGroup = new SiddhiBlockingQueue<SiddhiQueue<InEvent>>();
    AtomicLong count = new AtomicLong(0);
    long start = 0;
    long queueIdentifier = 0;
    private String[] streamDefinitionArray;
    private String siddhiQuery;
    private SiddhiConfiguration siddhiConfiguration;
    private Map<String, SiddhiManager> dynamicSiddhiManagerImpl = new LinkedHashMap<String, SiddhiManager>();
    private List<SiddhiEventConsumer> siddhiEventConsumerList = new ArrayList<SiddhiEventConsumer>();
    private SiddhiQueue<InEvent> currentProcessedEventQueue = new SiddhiQueue<InEvent>();
    private SiddhiQueue<InEvent> nextProcessedEventQueue = new SiddhiQueue<InEvent>();
    private Logger log = Logger.getLogger(SiddhiWrapper.class);
    private long lastEventTimeStamp;
    private long middleEventTimeStamp;
    private long withinTimeInterval = 2000;
    private AtomicInteger runningSiddhiManagerCount = new AtomicInteger(0);
    private int maxSiddhiManagerCount = 2;
    private SiddhiWrapper siddhiWrapper;

    public void createExecutionPlan(String[] streamDefinitionArray, String siddhiQuery,
                                    SiddhiConfiguration siddhiConfiguration, int maxSiddhiManagerCount) {

        this.streamDefinitionArray = streamDefinitionArray;
        this.siddhiQuery = siddhiQuery;
        this.siddhiConfiguration = siddhiConfiguration;
        this.maxSiddhiManagerCount = maxSiddhiManagerCount;
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
        Thread eventPublisher = new Thread(new EventPublisher(siddhiManager, queryReference));
        eventPublisher.setPriority(Thread.MAX_PRIORITY);
        eventPublisher.start();
    }

    public void shutdown() {
        for (SiddhiManager siddhiManager : dynamicSiddhiManagerImpl.values()) {
            siddhiManager.shutdown();
        }
    }

    public void sentEvents(String streamId, Object[] event, long currentTimeInMillis) {

        if (currentProcessedEventQueue.size() == 0) {
            lastEventTimeStamp = currentTimeInMillis + withinTimeInterval + withinTimeInterval;
            middleEventTimeStamp = currentTimeInMillis + withinTimeInterval;
            currentProcessedEventQueue = nextProcessedEventQueue;
            nextProcessedEventQueue = new SiddhiQueue<InEvent>();
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));

        } else if (currentTimeInMillis >= middleEventTimeStamp && currentTimeInMillis <= lastEventTimeStamp) {
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
            nextProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
        } else if (currentTimeInMillis > lastEventTimeStamp) {
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

            queueIdentifier++;
            currentProcessedEventQueue.setId(queueIdentifier);

            siddhiBlockingQueueGroup.put(currentProcessedEventQueue);
            currentProcessedEventQueue = new SiddhiQueue<InEvent>();
            spinSiddhiManagerInstance();
        } else {
            currentProcessedEventQueue.put(new InEvent(streamId, currentTimeInMillis, event));
        }

    }

    class EventPublisher implements Runnable {

        SiddhiManager siddhiManager;
        String queryReference;
        InputHandler inputHandler;
        int uniqueId;

        EventPublisher(SiddhiManager siddhiManager, String queryReference) {
            this.siddhiManager = siddhiManager;
            inputHandler = siddhiManager.getInputHandler("sensorStream");
            this.queryReference = queryReference;
            Random rand = new Random();
            uniqueId = rand.nextInt(500000) + 1;
        }

        @Override
        public void run() {
            long lastId = -1;
            while (true) {
                SiddhiQueue<InEvent> siddhiQueue = siddhiBlockingQueueGroup.peek();
                if (siddhiQueue != null) {
                    if (lastId == -1 || ((siddhiQueue.getId() != lastId + 1) && (siddhiQueue.getId() != lastId + 2))) {
                        siddhiQueue = siddhiBlockingQueueGroup.poll();
                        if (siddhiQueue != null) {
                            lastId = siddhiQueue.getId();

                            siddhiManager.addCallback(queryReference, new QueryCallback() {
                                @Override
                                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                                    for (SiddhiEventConsumer siddhiEventConsumer : siddhiEventConsumerList) {
                                        siddhiEventConsumer.receiveEvents(timeStamp, inEvents, removeEvents);

                                    }
                                }
                            });

                            runningSiddhiManagerCount.incrementAndGet();
                            InEvent siddhiEvent = null;
                            while (siddhiQueue.size() > 0) {
                                siddhiEvent = siddhiQueue.poll();
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

                            if (siddhiEvent != null) {
                                return;
                            }

                        }
                    }
                }
            }
        }
    }

}
