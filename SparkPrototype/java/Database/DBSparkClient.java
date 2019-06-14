package Database;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DBSparkClient {
    public String TARGET_URL;
    public String TARGET_USER;
    public String TARGET_PASSWORD;
    private String driver;

    public DBSparkClient(String url, String user, String password, String driver){
        this.TARGET_URL = url;
        this.TARGET_USER = user;
        this.TARGET_PASSWORD = password;
        this.driver = driver;
    }

    public void createTableInDB(String tableName, StructType schema){
        try(Connection connection = DriverManager.getConnection(TARGET_URL, TARGET_USER, TARGET_PASSWORD)) {
            Class.forName(driver);
            Statement statement = connection.createStatement();

            String tableCreateQuery = tableCreateStatement(tableName, schema);
            statement.execute(tableCreateQuery);

        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static String tableCreateStatement(String tableName, StructType schema){
        scala.collection.Iterator<StructField> cols = schema.iterator();
        StructField first = cols.next();
        String sql = "CREATE TABLE " + tableName + " ( " +
                first.name() + " " + getType(first.dataType().sql()) + " NOT NULL";
        while(cols.hasNext()){
            StructField col = cols.next();
            sql += ", "  +  col.name() + " " + getType(col.dataType().sql()) + " NOT NULL";
        }
        sql += ")";
        System.out.println("CreateTableSQL: " + sql);
        return sql;

    }

    private static String getType(String datatype){
        switch (datatype){
            case "STRING":
                return "varchar(255)";
            case "BIGINT":
                return datatype;
            case "DOUBLE":
                return datatype;
            case "TIMESTAMP":
                return datatype;
            default:
                return "varchar(255)";

        }
    }


    public static String InsertIntoSqlStatement(Row row, String tableName, StructType schema) {
        String sql = "INSERT INTO " + tableName + " (";
        scala.collection.Iterator<StructField> cols = schema.iterator();
        String first = cols.next().name();
        sql += first;
        while (cols.hasNext()) {
            sql += ", " + cols.next().name();
        }
        sql += ")";


        sql += " VALUES (";
        sql += "'" + rowValueToString(row.get(0)) + "'";
        for (int i = 1; i < row.length(); i++) {
            sql += ", " + rowValueToString(row.get(i));
        }
        sql += " );";
        return sql;
    }

    private static String rowValueToString(Object value){
        if (value == null){
            return "0";
        }
        return value.toString();

    }

    public String getTARGET_URL() {
        return TARGET_URL;
    }

    public String getTARGET_USER() {
        return TARGET_USER;
    }

    public String getTARGET_PASSWORD() {
        return TARGET_PASSWORD;
    }
}
