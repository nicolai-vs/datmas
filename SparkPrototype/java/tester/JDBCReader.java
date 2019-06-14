package tester;

import org.json.JSONObject;

import java.io.IOException;
import java.sql.*;

public class JDBCReader {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        String TARGET_HOST = "10.10.10.39";
        String TARGET_PORT = "9092";
        String TARGET_SCHEMA = "SparkServing";
        String TARGET_URL = "jdbc:h2:tcp://" + TARGET_HOST + ":" + TARGET_PORT + "/mem:" + TARGET_SCHEMA + ";DB_CLOSE_DELAY=-1";
        String TARGET_USER = "admin";
        String TARGET_PASSWORD = "admin";
        String TARGET_TABLE =  "PhoneEvent";
        String blackList  = "CATALOGS " + "COLLATIONS " + "COLUMNS " + "COLUMN_PRIVILEGES " + "CONSTANTS " +
                "CONSTRAINTS " + "CROSS_REFERENCES " + "DOMAINS " + "FUNCTION_ALIASES " + "FUNCTION_COLUMNS " +
                "HELP " + "INDEXES " + "IN_DOUBT " + "KEY_COLUMN_USAGE " + "LOCKS " + "QUERY_STATISTICS " +
                "REFERENTIAL_CONSTRAINTS " + "RIGHTS " + "ROLES " + "SCHEMATA " + "SEQUENCES " + "SESSIONS " + "SESSION_STATE " +
                "SETTINGS " + "SYNONYMS " + "TABLES " + "TABLE_CONSTRAINTS " + "TABLE_PRIVILEGES " + "TABLE_TYPES " +
                "TRIGGERS " + "TYPE_INFO " + "USERS " + "VIEWS ";
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(TARGET_URL, TARGET_USER, TARGET_PASSWORD);

        ResultSet tables = conn.getMetaData().getTables(TARGET_SCHEMA.toUpperCase(), null, null, null);
        String tableName = "";
        while (tables.next()){
            if(!blackList.contains(tables.getString(3))){
                tableName = tables.getString(3);
                System.out.println(tableName);
            }

        }


        ResultSet dbContent = conn.createStatement().executeQuery("select * from " + tableName);
        ResultSetMetaData metadata = dbContent.getMetaData();
        int columnCount = metadata.getColumnCount();
        String[] colNames = new String[columnCount];
        for(int i = 1; i <= columnCount; i++){
            colNames[i-1] = metadata.getColumnName(i);
        }

        // File h2DataFile = new File("src/main/data/h2DataFile");
        // FileWriter h2DataWriter = new FileWriter(h2DataFile, true);
        int counter = 0;
        while (dbContent.next()){
            JSONObject json = new JSONObject();
            for (int i = 1; i <= dbContent.getMetaData().getColumnCount(); i++) {
                json.put(colNames[i-1], dbContent.getString(i));
            }
            System.out.println(json.toString());
            counter++;
            // h2DataWriter.write(json.toString() + "\n");
        }
        System.out.println("Counter: " + counter);
        // h2DataWriter.close();






    }

}
