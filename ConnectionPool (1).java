package org.example.cloudSQL;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class ConnectionPool {

    private static final String INSTANCE_CONNECTION_NAME =
            "gcs-bigquery-test-415004:us-central1:naga-mysql";
    private static final String DB_USER = "naga";
    private static final String DB_PASS = "Chandra@420";
    private static final String DB_NAME = "my_database";

    public static DataSource createConnectionPool() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:mysql://34.135.216.177:3306/%s", DB_NAME));
        config.setUsername(DB_USER);
        config.setPassword(DB_PASS);
        config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.mysql.SocketFactory");
        config.addDataSourceProperty("cloudSqlInstance", INSTANCE_CONNECTION_NAME);

        return new HikariDataSource(config);
    }
}
