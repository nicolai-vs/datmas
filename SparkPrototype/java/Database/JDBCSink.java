package Database;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCSink extends ForeachWriter<Row> {
    private  String tableName;
    private StructType schema;
    private String url;
    private String user;
    private String password;


    private String driver;
    private Connection connection;
    private Statement statement;


    public JDBCSink( String tableName, StructType schema, String url, String user, String password, String driver) {
        this.tableName = tableName;
        this.schema = schema;
        this.url = url;
        this.user = user;
        this.password = password;
        this.driver = driver;
    }

    @Override
    public boolean open(long partitionID, long version) {
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return true;
    }

    @Override
    public void process(Row row) {
        try {
            String sql = DBSparkClient.InsertIntoSqlStatement(row, tableName, schema);
            statement.execute(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(Throwable throwable) {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public StructType getSchema() {
        return schema;
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }

    public static final class Builder {
        private  String tableName;
        private StructType schema;
        private String url;
        private String user;
        private String password;
        private String driver;

        private Builder() {
        }

        public static Builder aJDBCSink() {
            return new Builder();
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withSchema(StructType schema) {
            this.schema = schema;
            return this;
        }

        public Builder withUrl(String url) {
            this.url = url;
            return this;
        }

        public Builder withUser(String user) {
            this.user = user;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withDriver(String driver) {
            this.driver = driver;
            return this;
        }

        public JDBCSink build() {
            return new JDBCSink(tableName, schema, url, user, password, driver);
        }
    }
}
