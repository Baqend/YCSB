package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import info.orestes.client.OrestesClient;
import info.orestes.client.OrestesObjectClient;
import info.orestes.common.typesystem.*;
import info.orestes.pluggable.types.data.OObject;
import info.orestes.predefined.OrestesClass;
import info.orestes.rest.conversion.ClassFieldHolder;
import info.orestes.rest.conversion.ClassHolder;
import java.util.*;

/**
 * Created by Michael on 04.08.2014.
 */
public class BaqendClient extends DB {

    public static final String HOST_PROPERTY = "http://localhost:8080/";
    public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";
    private String table;
    private Bucket bucket;
    private OrestesClass schema;
    private OrestesClient client;

    @Override
    public void init() throws DBException {

        Properties props = getProperties();
        String url = props.getProperty("server.url",
                HOST_PROPERTY);

        try {
            client = new OrestesObjectClient(url);
        } catch (Exception e) {
            System.err.println("Could not initialize Baqend client : "
                    + e.toString());
        }

        table = props.getProperty("table", TABLENAME_PROPERTY_DEFAULT);
        bucket = new Bucket(table);
        ClassHolder classHolder = new ClassHolder(bucket, BucketAcl.createDefault());
        ClassFieldHolder values = new ClassFieldHolder("values", Bucket.MAP, Bucket.STRING, Bucket.STRING);
        classHolder.init(values);

        schema = client.getSchema().add(classHolder);
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        client.getClient().destroy();
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        OObject obj = client.load(new ObjectId(bucket, key));

        try {
            if (fields != null) {
                for (String s : fields) {
                    String v = obj.getValue(s).toString();
                    result.put(s, new StringByteIterator(v));
                }
                ;
            }
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        try {
            String query = "{\"_id\":{\"$gte\":" + startkey + "}}\"";
            List<ObjectInfo> ids = client.executeQuery(bucket, query);
            HashMap<String, ByteIterator> values;

            for (ObjectInfo info : ids) {
                OObject obj = client.load(info);
                values = new HashMap<>();

                if (fields != null) {
                    for (String s : fields) {
                        String v = obj.getValue(s).toString();
                        values.put(s, new StringByteIterator(v));
                    }
                }
                result.add(values);
            }
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        OObject obj = schema.newInstance(new ObjectId(bucket, key), Version.ANY);
        obj.setValue(schema.getField("values"), values);
        try {
            client.store(obj);
            return 0;
        } catch (Exception e) {
            return 1;
        }

    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        OObject obj = schema.newInstance(new ObjectId(bucket, key), Version.NEW);
        obj.setValue(schema.getField("values"), values);
        try {
            client.store(obj);
            return 0;
        } catch (Exception e) {
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
}
