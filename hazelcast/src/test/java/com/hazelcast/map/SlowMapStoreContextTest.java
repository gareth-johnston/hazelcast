package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.mapstore.MapStoreTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SlowMapStoreContextTest extends HazelcastTestSupport {

    @Test
    public void testWIP() {
        final String mapName = randomMapName();
        final Config config = getConfig();
        final MapConfig mapConfig = config.getMapConfig(mapName);
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        final SlowMapStore slowMapStore = new SlowMapStore();
        final MapStoreConfig implementation = mapStoreConfig.setImplementation(slowMapStore);
        mapConfig.setMapStoreConfig(implementation);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        HazelcastInstance node1 = factory.newHazelcastInstance(config);

        IMap<Object, Object> map = node1.getMap(mapName);
        map.set(1, 1);

        factory.newHazelcastInstance(config);
        // wait migrations end
        waitAllForSafeState(factory.getAllHazelcastInstances());

        Assertions.assertEquals(1, SlowMapStore.initCalled.get(), "init method of map store should be called once");
    }

    public static class SlowMapStore extends MapStoreTest.TestMapStore implements MapLoaderLifecycleSupport {
        static final AtomicInteger initCalled = new AtomicInteger();
        @Override
        public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
            initCalled.incrementAndGet();
            super.init(hazelcastInstance, properties, mapName);
        }
    }
}
