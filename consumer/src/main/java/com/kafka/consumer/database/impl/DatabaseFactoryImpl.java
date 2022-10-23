package com.kafka.consumer.database.impl;

import com.kafka.consumer.database.DatabaseFactory;
import com.kafka.message.avro.Dataset;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DatabaseFactoryImpl<T> implements DatabaseFactory<T> {

	private static final Logger LOG = LoggerFactory.getLogger(DatabaseFactoryImpl.class);

	private static Connection connection;

	private static String insertStatement = "insert into MyTest(consumer_key, message, TimeDate, NumberIdentity) values(?,?,?,?); ";

	private DatabaseFactoryImpl(){}

	public DatabaseFactoryImpl(Connection connection ){
		this.connection = connection;
	}

	@Override
	public void initConnection() {

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

	@Override
	public List<Dataset> subscribeAvro(Map<String, Dataset> avroDatas) {

		//순서 보장
		List<Dataset> transaction = new LinkedList<>();

		for(Map.Entry<String, Dataset>  map : avroDatas.entrySet()){

			transaction.add(Dataset.newBuilder()
					.setKey(map.getKey())
					.setMessage(map.getValue().getMessage())
					.setNumber(map.getValue().getNumber())
					.setTimestamp(map.getValue().getTimestamp())
					.build());
		}

		return transaction;

	}

	@Override
	public void insertData(List<Dataset> data) {

		PreparedStatement statement = null;
		try {
			statement = connection.prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS);
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

		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				LOG.error("Problems closing database", e);
			}
		}

	}
}
