package com.kafka.db;

import com.kafka.Dataset;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class MyDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(MyDatabase.class);

    private static Connection connection;
    private static String insertStatement = "insert into MyTest(consumer_key, message, TimeDate, NumberIdentity) values(?,?,?,?); ";


    public static void initConnection(){

        MysqlDataSource ds = new MysqlDataSource();
        ds.setServerName("127.0.0.1");
        ds.setPort(3306);
        ds.setUser("wonsj");
        ds.setPassword("sjwood4111");
        ds.setDatabaseName("MyData");

        try {
            ds.setCharacterEncoding("utf-8");
            connection = ds.getConnection();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
            LOG.error("Problem opening connection : [{}]", throwables.getSQLState());
        }

    }

    public static void getDataFromConsumer(LinkedHashMap<String,Dataset> linkedMap){

        List<Dataset> transaction = linkedMap.entrySet()
                .parallelStream()
                .map((map) ->
                        Dataset.newBuilder()
                                .setKey(map.getKey())
                                .setMessage(map.getValue().getMessage())
                                .setNumber(map.getValue().getNumber())
                                .setTimestamp(map.getValue().getTimestamp())
                                .build()

                ).collect(Collectors.toList());

        MyDatabase.insertData(transaction);
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.error("Problems closing database", e);
            }
        }

    }

    private static synchronized void insertData(List<Dataset> data) {

        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(insertStatement,Statement.RETURN_GENERATED_KEYS);
            connection.setAutoCommit(false);
            for (Dataset transaction : data) {
                statement.setString(1, String.valueOf(transaction.getKey()));
                statement.setString(2,transaction.getMessage().toString());
                statement.setString(3, String.valueOf(transaction.getNumber()));
                statement.setTimestamp(4, new java.sql.Timestamp(transaction.getTimestamp()));

                statement.addBatch();
            }
            statement.executeBatch();

            connection.commit();
        } catch (SQLException e) {
            LOG.error("-> INSERT ERROR : [{}]", e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if(statement!= null) {
                    statement.close();
                }
            } catch (SQLException e) {
                LOG.error("-> CONNECTION CLOSE ERROR : [{}]", e.getSQLState());
            }
        }
    }
}
