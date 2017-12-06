package com.hector;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        Cluster myCluster = HFactory.getOrCreateCluster("Test Cluster", "localhost:9160");

        KeyspaceDefinition keyspaceDef = myCluster.describeKeyspace("TestKeyspace2");

        if (keyspaceDef == null) {
            ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("TestKeyspace2",
                    "Person",
                    ComparatorType.BYTESTYPE);

            KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("TestKeyspace2",
                    ThriftKsDef.DEF_STRATEGY_CLASS,
                    1,
                    Arrays.asList(cfDef));
            myCluster.addKeyspace(newKeyspace, true);
        }

//        Keyspace ksp = HFactory.createKeyspace("TestKeyspace2", myCluster);
//
//        ColumnFamilyTemplate<String, String> template =
//                new ThriftColumnFamilyTemplate<String, String>(ksp,
//                        "Person",
//                        StringSerializer.get(),
//                        StringSerializer.get());
//
//        ColumnFamilyUpdater<String, String> updater = template.createUpdater("wang");
//        updater.setString("domain", "www.datastax.com");
//        updater.setLong("time", System.currentTimeMillis());
    }


}
