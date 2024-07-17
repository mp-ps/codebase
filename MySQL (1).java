package org.example.cloudSQL;

import javax.sql.DataSource;
import java.sql.*;

public class MySQL {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        DataSource dataSource = ConnectionPool.createConnectionPool();

        ResultSet rs = dataSource.getConnection().prepareStatement("select * from my_table").executeQuery();

        while (rs.next()) {
            System.out
                    .println("name: " + rs.getString("name") + " age:" + rs.getString("age"));
        }

//        Class.forName("com.mysql.cj.jdbc.Driver");
//        Connection connection = DriverManager.getConnection(
//                "jdbc:mysql://34.135.216.177:3306/my_database?socketFactory=com.google.cloud.sql.mysql.SocketFactory&cloudSqlInstance=gcs-bigquery-test-415004:us-central1:naga-mysql&user=naga&password=Chandra@420");
////                "naga", "Chandra@420");
//        Statement statement;
//        statement = connection.createStatement();
//        ResultSet resultSet;
//        resultSet = statement.executeQuery(
//                "select * from my_table");
//        int code;
//        String title;
//        while (resultSet.next()) {
//            code = resultSet.getInt("age");
//            title = resultSet.getString("name").trim();
//            System.out.println("Code : " + code
//                    + " Title : " + title);
//
//        }
//        resultSet.close();
//        statement.close();
//        connection.close();
    }
}
