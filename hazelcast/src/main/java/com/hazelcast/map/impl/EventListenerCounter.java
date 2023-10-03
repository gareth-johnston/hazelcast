/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registry for counters which holds
 * registered event listener count per map.
 * <p>
 * Created only 1 instance per {@link MapService}
 */
public class EventListenerCounter {

    private final ConcurrentMap<String, AtomicInteger> counterPerMap
            = new ConcurrentHashMap<>();
    private static final ILogger logger = Logger.getLogger(EventListenerCounter.class);

    public EventListenerCounter() {
    }

    public AtomicInteger getOrCreateCounter(String mapName) {
        logger.warning("XXX: getOrCreateCounter for map= " + mapName);
        return counterPerMap.computeIfAbsent(mapName,
                s -> new AtomicInteger());
    }

    public void removeCounter(String mapName,
                              AtomicInteger counter) {
        logger.warning("XXX: Removing counter for map= " + mapName);
        counterPerMap.remove(mapName, counter);
    }

    public void incCounter(String mapName) {
        logger.warning("XXX: Incrementing counter for map= " + mapName);
        getOrCreateCounter(mapName).incrementAndGet();
    }

    public void decCounter(String mapName) {
        AtomicInteger counter = counterPerMap.get(mapName);
        if (counter != null) {
            int count = counter.decrementAndGet();

            assert count >= 0 : "count should not be negative : was " + count + " for map " + mapName;
        }
    }
}
