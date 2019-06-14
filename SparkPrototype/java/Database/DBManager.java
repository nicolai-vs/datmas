package Database;

import org.h2.tools.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBManager {

    public static final String PORT = "9092";
    public static final String SCHEMANAME = "SparkServing";
    public static final String JDBC_H2_TCP_URL = "jdbc:h2:mem:" + SCHEMANAME + ";DB_CLOSE_DELAY=-1";
    public static final String DB_USER_NAME = "admin";
    public static final String DB_PASSWORD = "admin";

    public static void main(String[] args) {
        DBManager.startDB();
    }

    public static void startDB() {
        try {
            Server.createTcpServer("-tcpPort", PORT, "-tcpAllowOthers").start();
            Class.forName("org.h2.Driver");
            try (Connection conn = DriverManager.getConnection(JDBC_H2_TCP_URL, DB_USER_NAME, DB_PASSWORD)) {
                Statement st = conn.createStatement();
                System.out.println("Connection Established: " + conn.getMetaData().getDatabaseProductName() + "/" + conn.getCatalog());
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void stopDB() throws SQLException {
        Server.shutdownTcpServer("tcp://localhost:" + PORT, "", true, true);
    }




}