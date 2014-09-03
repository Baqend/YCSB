package com.yahoo.ycsb.db;

import com.codahale.metrics.MetricRegistry;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import info.orestes.client.OrestesObjectClient;
import info.orestes.common.typesystem.*;
import info.orestes.pluggable.types.data.OObject;
import info.orestes.predefined.OrestesClass;
import info.orestes.rest.conversion.ClassFieldHolder;
import info.orestes.rest.conversion.ClassHolder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Created by Michael on 04.08.2014.
 */
public class BaqendClient extends DB {

    public static final String HOST_PROPERTY = "http://benchmark.baqend.com:80/";
    public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";
    private volatile static String table;
    private volatile static Bucket bucket;
    private volatile static MetricRegistry metricRegistry;

    private static volatile OrestesClass schema;
    private static volatile OrestesObjectClient client;
    private static AtomicInteger threadCount = new AtomicInteger();

    @Override
    public void init() throws DBException {
        synchronized (BaqendClient.class) {
            if (client == null) {

                    Properties props = getProperties();
                    String url = props.getProperty("server.url",
                            HOST_PROPERTY);

                    try {
                        client = new OrestesObjectClient(url);
                        metricRegistry = client.getMetricRegistry();
          /*              ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                                .convertRatesTo(TimeUnit.SECONDS)
                                .convertDurationsTo(TimeUnit.MILLISECONDS)
                                .build();
                        reporter.start(10, TimeUnit.SECONDS);*/

                    } catch (Exception e) {
                        System.err.println("Could not initialize Baqend client : "
                                + e.toString());
                        e.printStackTrace();
                    }

                    table = props.getProperty("table", TABLENAME_PROPERTY_DEFAULT);
                    bucket = new Bucket(table);
                    ClassHolder classHolder = new ClassHolder(bucket, BucketAcl.createDefault());
                    ClassFieldHolder values = new ClassFieldHolder("values", Bucket.MAP, Bucket.STRING, Bucket.STRING);
                    ClassFieldHolder timestamp = new ClassFieldHolder("time", Bucket.INTEGER);
                    classHolder.init(values, timestamp);

                    schema = client.getSchema().add(classHolder);
                }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (threadCount.incrementAndGet() == Long.parseLong(getProperties().getProperty("threadcount", "16"))) {
            try {
                Double hits = Double.valueOf(metricRegistry.counter("cache-hits").getCount());
                Double misses = Double.valueOf(metricRegistry.counter("cache-misses").getCount());
                BufferedWriter writer = new BufferedWriter(new FileWriter(("cache_c_i_workloada.txt"), true));
                writer.write("{" + StalenessDetector.countStaleReads() + "," + hits + ","+ misses + "," + (hits / (hits + misses)) + "}");
                writer.flush();
                writer.close();

                System.out.println("stale_reads = " + StalenessDetector.countStaleReads());
                System.out.println("hit_ratio=" + (hits / (hits + misses)));
            } catch (IOException e) {
                e.printStackTrace();
            }


            client.getClient().destroy();
        }
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            long t = StalenessDetector.generateVersion();
            OObject obj = client.load(new ObjectId(bucket, key));
            StalenessDetector.testForStaleness(key, (long) obj.getValue("time"), t);

            if (fields != null) {
                for (String s : fields) {
                    String v = ((Map<String, String>) obj.getValue("values")).get(s);
                    result.put(s, new StringByteIterator(v));
                }
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        try {
            String query = "{\"_id\":{\"$gte\":\"" + startkey + "\"}}";
            List<ObjectInfo> ids = client.executeQuery(bucket, query, 0, recordcount);
            List<ObjectInfo> newIds = new LinkedList<>();
            for (ObjectInfo id : ids) {
                newIds.add(new ObjectInfo(id.getId(), Version.ANY));
            }

            HashMap<String, ByteIterator> values;
            Stream<CompletableFuture<OObject>> stream = client.loadAllInfos(newIds.stream());
            values = new HashMap<>();

            stream.map(CompletableFuture::join).forEach(obj -> {
                if (fields != null) {
                    for (String s : fields) {
                        String v = null;

                        try {
                            v = ((Map<String, String>) obj.getValue("values")).get(s);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        values.put(s, new StringByteIterator(v));
                    }
                }
                result.add(values);
            });

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            OObject obj = schema.newInstance(new ObjectId(bucket, key), Version.ANY);
            obj.setValue(schema.getField("values"), convertMap(values));

            Long t = StalenessDetector.generateVersion();
            obj.setValue(schema.getField("time"), t);

            client.store(obj);
            StalenessDetector.addVersion(key, t);
            StalenessDetector.addWriteAcknowledgement(key, StalenessDetector.generateVersion());


            return 0;
        } catch (Exception e) {
            return 1;
        }

    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            OObject obj = schema.newInstance(new ObjectId(bucket, key), Version.NEW);
            obj.setValue(schema.getField("values"), convertMap(values));
            Long t = StalenessDetector.generateVersion();
            obj.setValue(schema.getField("time"), t);
            client.store(obj);
            StalenessDetector.addVersion(key, t);
            StalenessDetector.addWriteAcknowledgement(key, StalenessDetector.generateVersion());

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int delete(String table, String key) {
        try {
            client.delete(new ObjectId(bucket, key));
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }

    private Map<String, String> convertMap(Map<String, ByteIterator> values) {
        Map<String, String> newValues = new HashMap<>(values.size());
        for (String k : values.keySet()) {
            newValues.put(k, Base64.getEncoder().encodeToString(values.get(k).toArray()));
        }
        return newValues;
    }
}
