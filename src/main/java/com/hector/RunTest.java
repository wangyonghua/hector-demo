package com.hector;


import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class RunTest {
    String clusterName = "test-cluster";
    String keyspace = "ks1";
    String cfName = "cf1";

    Cluster cluster = HFactory.getOrCreateCluster(clusterName, "localhost:9160");

    Keyspace ksp = HFactory.createKeyspace(keyspace, cluster);

    ColumnFamilyTemplate<String, String> template = new ThriftColumnFamilyTemplate<String, String>(ksp, cfName, StringSerializer.get(), StringSerializer.get());


    @Test
    public void createKeySpaceAndCf() {
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace, cfName, ComparatorType.UTF8TYPE);
        // 下面两行我加的，为了让key和value以UTF8格式保存
        cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
        cfDef.setDefaultValidationClass(ComparatorType.UTF8TYPE.getClassName());

        // 副本数我修改为1，官方为2，单机无法测试使用
        KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspace, ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(cfDef));
        cluster.addKeyspace(newKeyspace, true);
    }

    @Test
    public void insert() {
        ColumnFamilyUpdater<String, String> updater = template.createUpdater("rowkey1");
        updater.setString("name", "heipark");
        try {
            template.update(updater);
        } catch (HectorException e) {
            System.out.println(e);
        }
    }

    @Test
    // 根据单个key查询所有columns
    public void testSliceQuery2() {
        SliceQuery<String, String, String> query = HFactory.createSliceQuery(ksp, StringSerializer.get(),
                StringSerializer.get(), StringSerializer.get()).
                setKey("rowkey1").setColumnFamily(cfName);

        ColumnSliceIterator<String, String, String> iterator =
                new ColumnSliceIterator<String, String, String>(query, null, "\uFFFF", false);

        while (iterator.hasNext()) {
            HColumn<String, String> next = iterator.next();
            String name = next.getName();
            String val = next.getValue();
            System.out.println("name=>" + name + ",val=>" + val);
            // do something
        }
    }

    @Test
    public void read() {
        try {
            ColumnFamilyResult<String, String> res = template.queryColumns("rowkey1");
            assertEquals("heipark", res.getString("name"));
        } catch (HectorException e) {
            System.out.println(e);
        }
    }

    @Test
    public void delete() {
        try {
            template.deleteColumn("rowkey1", "name");
        } catch (HectorException e) {
            // do something
        }
    }
}
