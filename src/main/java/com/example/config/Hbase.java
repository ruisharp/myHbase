package com.example.config;

import com.spring4all.spring.boot.starter.hbase.api.HbaseTemplate;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicStampedReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;


/**
 * @author allen
 */
@org.springframework.context.annotation.Configuration
public class Hbase {
    @Bean
    public Connection getHbaseConnect() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.0.204");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("log4j.logger.org.apache.hadoop.hbase", "WARN");
        conf.set(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, "");
        conf.set(HConstants.HBASE_CLIENT_IPC_POOL_TYPE, "RoundRobin");
        Connection connection = ConnectionFactory.createConnection(conf);
        AtomicStampedReference
        return connection;
    }

    @Bean
    public Admin getHbaseAdmin(Connection connection) throws IOException{
        Admin admin = connection.getAdmin();
        return admin;
    }

}