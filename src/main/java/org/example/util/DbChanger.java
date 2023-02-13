package org.example.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DbChanger {
    public static void main(String[] args) {
        try(Connection conn = DriverManager.getConnection(
                "jdbc:h2:tcp://localhost:9092/nio:~/db", "", "");
            Statement stmt = conn.createStatement()) {
            String insertSql = "insert into person values " +
                    "(000000, 'Linda', 'Zinkina', 'Moscow')";
            stmt.executeUpdate(insertSql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
