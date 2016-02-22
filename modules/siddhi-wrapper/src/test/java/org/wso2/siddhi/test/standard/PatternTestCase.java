/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.siddhi.test.standard;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.wrapper.SiddhiWrapper;
import org.wso2.siddhi.wrapper.util.SiddhiEventConsumer;


public class PatternTestCase {
    static final Logger log = Logger.getLogger(PatternTestCase.class);

    private int eventCount;
    private long value;
    private boolean eventArrived;
    long startingTime = 0;
    long endingTime = 0;

    @Before
    public void init() {
        eventCount = 0;
        value = 0;
        eventArrived = false;
        startingTime = 0;
        endingTime = 0;
    }


    @Test
    public void testPatternWithinQuery1() throws InterruptedException {
        log.info("testPatternWithin1 - OUT 1");

        SiddhiWrapper siddhiWrapper = new SiddhiWrapper();
        String[] streamDefinitions = new String[]{" define stream Stream1 (symbol string, price float, volume int ); ",
                " define stream Stream2 (symbol string, price float, volume int );"};
        String query = "" +
                "from every e1=Stream1[price>20] -> e2=Stream2[price>e1.price] within 1 sec " +
                "select e1.symbol as symbol1, e2.symbol as symbol2 " +
                "insert into OutputStream ;";

        siddhiWrapper.createExecutionPlan(streamDefinitions, query, null, 2);
        siddhiWrapper.registerCallback(new SiddhiEventConsumer() {
            @Override
            public void receiveEvents(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
            }
        });

        siddhiWrapper.sentEvents("Stream1", new Object[]{"WSO2", 55.6f, 100});
        Thread.sleep(2100);
        siddhiWrapper.sentEvents("Stream1", new Object[]{"GOOG", 54f, 100});
        //Thread.sleep(500);
        siddhiWrapper.sentEvents("Stream2", new Object[]{"IBM", 55.7f, 100});
        Thread.sleep(500);
        siddhiWrapper.sentEvents("Stream1", new Object[]{"IBM2", 55.7f, 100});
        Thread.sleep(5000);

        siddhiWrapper.shutdown();

        org.junit.Assert.assertEquals("Event arrived", true, eventArrived);

    }
}
