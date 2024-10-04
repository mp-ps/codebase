import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {
        // Step 1: Initialize HikariCP DataSource
        DataSource dataSource = HikariCPDataSource.getDataSource();
        CloudSQLExecutor sqlExecutor = new CloudSQLExecutor(dataSource);

        // Step 2: Read data from BigQuery
        BigQueryReader reader = new BigQueryReader();
        List<String[]> data = reader.readBigQueryData("SELECT col1, col2, col3 FROM your_bigquery_table");

        // Step 3: Batch data
        BatchProcessor batchProcessor = new BatchProcessor();
        List<List<String[]>> batches = batchProcessor.createBatches(data, 1000);  // Batch size of 1000

        // Step 4: Construct and execute queries for each batch
        SQLQueryBuilder queryBuilder = new SQLQueryBuilder();
        for (List<String[]> batch : batches) {
            String sql = queryBuilder.buildInsertQuery(batch);
            sqlExecutor.executeQuery(sql);  // Execute the batch insert query
        }

        // Step 5: Close HikariCP DataSource
        HikariCPDataSource.close();
    }

    // Non-public class for HikariCP DataSource
    static class HikariCPDataSource {
        private static HikariDataSource dataSource;

        public static DataSource getDataSource() {
            if (dataSource == null) {
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl("jdbc:mysql://your-cloudsql-instance/your-db");
                config.setUsername("your-username");
                config.setPassword("your-password");
                config.addDataSourceProperty("cachePrepStmts", "true");
                config.addDataSourceProperty("prepStmtCacheSize", "250");
                config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
                config.setMaximumPoolSize(10);  // Adjust pool size as needed
                dataSource = new HikariDataSource(config);
            }
            return dataSource;
        }

        public static void close() {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    // Non-public class for executing SQL on Cloud SQL
    static class CloudSQLExecutor {
        private DataSource dataSource;

        public CloudSQLExecutor(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public void executeQuery(String sql) throws Exception {
            try (Connection connection = dataSource.getConnection();
                 Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql);
            }
        }
    }

    // Non-public class for building SQL queries
    static class SQLQueryBuilder {
        public String buildInsertQuery(List<String[]> batch) {
            StringBuilder query = new StringBuilder("INSERT INTO my_table (col1, col2, col3) VALUES ");

            for (String[] row : batch) {
                query.append("(");
                query.append("'").append(row[0]).append("',");
                query.append("'").append(row[1]).append("',");
                query.append("'").append(row[2]).append("'");
                query.append("),");
            }

            // Remove the trailing comma
            query.setLength(query.length() - 1);

            // Add ON DUPLICATE KEY UPDATE clause
            query.append(" ON DUPLICATE KEY UPDATE ");
            query.append("col1 = VALUES(col1), ");
            query.append("col2 = VALUES(col2);");

            return query.toString();
        }
    }

    // Non-public class for reading data from BigQuery
    static class BigQueryReader {
        public List<String[]> readBigQueryData(String query) throws InterruptedException {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            TableResult result = bigquery.query(queryConfig);

            List<String[]> rows = new ArrayList<>();
            result.iterateAll().forEach(row -> {
                String[] values = new String[]{
                    row.get("col1").getStringValue(),
                    row.get("col2").getStringValue(),
                    row.get("col3").getStringValue()
                };
                rows.add(values);
            });

            return rows;
        }
    }

    // Non-public class for processing batches
    static class BatchProcessor {
        public List<List<String[]>> createBatches(List<String[]> data, int batchSize) {
            List<List<String[]>> batches = new ArrayList<>();
            for (int i = 0; i < data.size(); i += batchSize) {
                int end = Math.min(data.size(), i + batchSize);
                batches.add(data.subList(i, end));
            }
            return batches;
        }
    }
}
