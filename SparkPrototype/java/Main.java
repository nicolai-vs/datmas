import Database.DBSparkClient;
import Database.JDBCSink;
import StreamProcessor.Processor;
import StreamProcessor.SchemaHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.spark.sql.functions.expr;


public class Main {


    private static SparkSession createSession(){
        SparkSession spark = SparkSession
                .builder()
                .master("local[10]")
                .appName("corpoSpark")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;

    }


    public static void main(String[] args) throws InterruptedException, IOException {
        System.setProperty("hadoop.home.dir", "C:\\Dev\\nicolai\\winutils\\hadoop-3.0.0");
        // System.setProperty("hadoop.home.dir", "C:\\dev\\winutils\\hadoop-3.0.0");
        FileInputStream streamProcessorProps = new FileInputStream("src/main/config/streamProcessorProps.properties");
        Properties propStreamProcessor = new Properties();
        propStreamProcessor.load(streamProcessorProps);




        String          HOST = propStreamProcessor.getProperty("host");
        int             PORT = Integer.parseInt(propStreamProcessor.getProperty("port"));
        String          WINDOW_SIZE = "5 seconds";
        String          WATERMARK_THRESHOLD = "2 seconds";
        String          SAMPLE = propStreamProcessor.getProperty("StreamSample"); // {"Ended":1554983954256,"Enter":1554983928285,"PhoneNbr":"8632032077","Handling":1554983928348}
        // String          SAMPLE = "{\"Event\":\"Handling\",\"TimeStampLong\":1554885569225,\"PhoneNbr\":\"9317211109\"}";
        SchemaHandler   SCHEMA = new SchemaHandler(SAMPLE);
        String          TABLE_NAME = propStreamProcessor.getProperty("tableName");

        FileInputStream dbSparkClientProps = new FileInputStream("src/main/config/dbSparkClientProps.properties");
        Properties propDbSparkClient = new Properties();
        propDbSparkClient.load(dbSparkClientProps);

        String          TARGET_URL = propDbSparkClient.getProperty("url");
        String          TARGET_USER = propDbSparkClient.getProperty("user");
        String          TARGET_PASSWORD = propDbSparkClient.getProperty("password");
        String          TARGET_DRIVER = propDbSparkClient.getProperty("driver");


        SparkSession    SPARK_SESSION = createSession();
        DBSparkClient   SPARK_CLIENT = new DBSparkClient(TARGET_URL, TARGET_USER, TARGET_PASSWORD, TARGET_DRIVER);




        JDBCSink SPARK_JDBC_SINK = JDBCSink.Builder.aJDBCSink()
                .withDriver(TARGET_DRIVER)
                .withUrl(TARGET_URL)
                .withUser(TARGET_USER)
                .withPassword(TARGET_PASSWORD)
                .withSchema(SCHEMA.getSchema())
                .withTableName(TABLE_NAME)
                .build();

        Processor processor = Processor.Builder.aProcessor()
                .withHost(HOST)
                .withPort(PORT)
                .withSchemaHandler(SCHEMA)
                .withSpark(SPARK_SESSION)
                .withTableName(TABLE_NAME)
                .withWindowSize(WINDOW_SIZE)
                .withWaterMarkThreshold(WATERMARK_THRESHOLD)
                .withDbSparkClient(SPARK_CLIENT)
                .withJdbcSink(SPARK_JDBC_SINK)
                .build();


        String sql = "SELECT * FROM PhoneEvent";
        // processor.Start(sql);
        processor.Start(sql);


        // join(processor.getStream().withWatermark("TimeStamp", "2 seconds"), operators.getStream().withWatermark("TimeStam", "2 seconds"));




    }


    private static void join(Dataset<Row> left , Dataset<Row> right){

        Dataset<Row> stream = left.join(right, expr(
                "PhoneNbr = PhoneNb AND " +
                "TimeStam >= TimeStamp AND " +
                "TimeStam <= TimeStamp + interval 2 seconds "
        ), "leftOuter");

        try {
            stream.writeStream()
                    .outputMode("update")
                    .format("console")
                    .option("truncate", "false")
                    .start()
                    .awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }

}
