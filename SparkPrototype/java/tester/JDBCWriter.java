package tester;

import Database.DBSparkClient;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCWriter {


    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        StructField[] cols = new StructField[]{
                new StructField("time", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("AVG_WAITINGTIME", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("AVG_PHONECALLDURATION", DataTypes.DoubleType, true, Metadata.empty())
        };
        StructType schema = new StructType(cols);
        String tableName = "PhoneEvent";
        String url = "jdbc:h2:tcp://10.10.10.39:9092/mem:SparkServing;DB_CLOSE_DELAY=-1";
        String user = "admin";
        String password = "admin";

        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement statement = conn.createStatement();
        String createStatementSQL = DBSparkClient.tableCreateStatement(tableName, schema);
        statement.execute(createStatementSQL);


        FileReader fileReader = new FileReader("src/main/data/h2DataFile");
        BufferedReader reader = new BufferedReader(fileReader);
        String line;
        while ((line = reader.readLine()) != null){
            JSONObject object = new JSONObject(line);
            String sql = insertStatment(object, tableName, schema);
            statement.execute(sql);
        }

        conn.close();
    }

    private static String insertStatment(JSONObject object, String tableName, StructType schema){
        String sql = "INSERT INTO " + tableName + " (";
        scala.collection.Iterator<StructField> cols = schema.iterator();
        String first = cols.next().name();
        sql += first;
        String values = " VALUES (";
        values += "'" + object.get(first) + "'";
        while (cols.hasNext()) {
            String name = cols.next().name();
            sql += ", " + name;
            values += ", '" + object.get(name) + "'";
        }
        sql += ")";
        values += ")";

        sql += values;
        return sql;
    }
}
