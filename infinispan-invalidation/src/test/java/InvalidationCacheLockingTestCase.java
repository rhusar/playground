import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.junit.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;

/**
 * @author Radoslav Husar
 */
public class InvalidationCacheLockingTestCase {

    @Test
    public void testInvalidationCacheForceWriteLock() throws Exception {
        Configuration c = new ConfigurationBuilder()
                .transaction()
                .clustering().cacheMode(CacheMode.INVALIDATION_SYNC)
                .transaction().transactionMode(TransactionMode.TRANSACTIONAL).transactionManagerLookup(new JBossStandaloneJTAManagerLookup()).lockingMode(LockingMode.PESSIMISTIC)
                .persistence().addStore(JdbcStringBasedStoreConfigurationBuilder.class).connectionPool().connectionUrl("jdbc:h2:mem:infinispan;DB_CLOSE_DELAY=-1")
                .fetchPersistentState(false)
                .shared(true)
                .table().tableNamePrefix("STRINGS_").idColumnName("id").idColumnType("VARCHAR").dataColumnName("datum").dataColumnType("BINARY").timestampColumnName("version").timestampColumnType("BIGINT")
                .build();

        EmbeddedCacheManager cm1 = new DefaultCacheManager(createGlobalConfiguration("node1"), c);
        Cache<String, String> c1 = cm1.getCache();
        TransactionManager tm1 = c1.getAdvancedCache().getTransactionManager();

        EmbeddedCacheManager cm2 = new DefaultCacheManager(createGlobalConfiguration("node2"), c);
        Cache<String, String> c2 = cm2.getCache();
        TransactionManager tm2 = c1.getAdvancedCache().getTransactionManager();

        String key = "k";
        c1.put(key, "some value");

        tm1.begin();
        c1.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key);

        Future<?> future = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                tm2.begin();
                c2.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key);

                fail();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        future.get();
    }

    private static GlobalConfiguration createGlobalConfiguration(String nodeName) {
        return new GlobalConfigurationBuilder()
                .defaultCacheName("session-cache")
                .transport().defaultTransport().addProperty("configurationFile", "default-jgroups-tcp.xml").nodeName(nodeName)
                .build();
    }
}
