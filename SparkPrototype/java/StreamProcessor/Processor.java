package StreamProcessor;

import Database.DBSparkClient;
import Database.JDBCSink;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Processor {
    private SchemaHandler schemaHandler;
    private SparkSession spark;
    private String host;
    private int port;
    private String tableName;
    private String windowSize;
    private String waterMarkThreshold;
    private DBSparkClient dbSparkClient;
    private JDBCSink jdbcSink;

    public Processor(SparkSession spark, String host, int port, SchemaHandler schemaHandler,
                     String tableName, String windowSize, String waterMarkThreshold, DBSparkClient dbSparkClient,
                     JDBCSink jdbcSink){
        this.schemaHandler = schemaHandler;
        this.spark = spark;
        this.host = host;
        this.port = port;
        this.tableName = tableName;
        this.windowSize = windowSize;
        this.waterMarkThreshold = waterMarkThreshold;
        this.dbSparkClient = dbSparkClient;
        this.jdbcSink = jdbcSink;
        createTempViewFromSocket();
    }


    public void Start(String sql){
        Dataset<Row> stream = spark.table("PHONEEVENT");
        stream = stream.selectExpr(CalculationProvider.selectExprProvider());

        stream = stream.withWatermark("timeStamp", waterMarkThreshold);

        Map<String, String> aggregations = CalculationProvider.aggregationMapProvider();
        stream = groupByWindow(stream, CalculationProvider.columnsToGroupByProvider()).agg(aggregations);

        // Need to rename them to be compatible with the H2 database.
        stream = renameAggregatedValues(stream, aggregations);

        // Formatting the window to only have one TimeStamp entry.
        stream = windowToDateMapper(stream);


        // H2Writer(stream);


        PrintToConsole(stream);

    }

    private Dataset<Row> filterExample(Dataset<Row> stream) {
        String filterWait = "avg_waitingTime" + " > 4";
        String filterDuration = "avg_Duration" + " > 9";
        stream = stream.filter(filterWait);
        stream = stream.filter(filterDuration);
        return stream;
    }
    private Dataset<Row> renameAggregatedValues(Dataset<Row> stream, Map<String, String> aggregations){
        for(Map.Entry<String, String> entry: aggregations.entrySet()){
            stream = renameAggregatedValue(stream, entry.getValue(), entry.getKey());
        }
        return stream;
    }
    private Dataset<Row> renameAggregatedValue(Dataset<Row> stream, String calculation, String colName){
        String old = calculation + "(" + colName + ")";
        String renamed = calculation + "_" + colName;
        return stream.withColumnRenamed(old, renamed);
    }
    public void StartPivoting(){
        // remember to NOT add timestamp before this method.
        Dataset<Row> stream = spark.table(tableName);
        stream = pivot(stream);
        stream = withColumnTimestamp(stream);
        stream = stream.withWatermark("timeStamp", "5 seconds");
        stream = stream.groupBy(
                stream.col("PhoneNbr"), functions.window(stream.col("timeStamp"), windowSize))
                .agg(
                        expr("(max(Handling) - max(Enter))").as("WaitingTime"),
                        expr("(max(Ended) - max(Handling))").as("Duration")
                );
        stream = stream.select("PhoneNbr", "WaitingTime", "Duration");
        PrintToConsole(stream);
    }

    private Dataset<Row> pivot(Dataset<Row> stream){
        String PIVOT_COL = "Event";
        StructField[] pivotStructure = new StructField[]{
                new StructField("Ended", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Enter", DataTypes.LongType, true, Metadata.empty()),
                new StructField("PhoneNbr", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Handling", DataTypes.LongType, true, Metadata.empty())
        };
        StructType pivotSchema = new StructType(pivotStructure);
        return stream.mapPartitions(
                (MapPartitionsFunction<Row, Row>) iterator -> {
                    ArrayList<Row> rows = new ArrayList<>();
                    while (iterator.hasNext()){
                        Row row = iterator.next();
                        Object[] values = new Object[pivotSchema.length()];
                        for(int i = 0; i < values.length; i++){
                            values[i] = null;
                        }
                        String event = row.getString(row.fieldIndex(PIVOT_COL));
                        int eventPosition = 1;
                        if (event.equals("Handling")){
                            eventPosition = 3;
                        }else if (event.equals("Ended")) {
                            eventPosition = 0;
                        }

                        values[eventPosition] = Long.parseLong(row.getString(row.fieldIndex("TimeStampLong")));
                        values[row.fieldIndex("PhoneNbr")] = Long.parseLong(row.getString(row.fieldIndex("PhoneNbr")));
                        // System.out.println("Objects are made: " + values);
                        rows.add(new GenericRowWithSchema(values, pivotSchema));
                        // rows.add(row);
                        // rows.add(Long.parseLong(row.getString(row.fieldIndex("TimeStampLong"))));
                    }
                    return rows.iterator();
                },
                RowEncoder.apply(pivotSchema));

    }

    private StructType windowToTimeStampStructType(StructType schema){
        Iterator<StructField> cols = schema.iterator();
        StructField[] encoderCols = new StructField[schema.length()];
        int index = 0;
        while (cols.hasNext()){
            StructField col = cols.next();
            if (col.name().equals("window")){
                encoderCols[index] = new StructField("timeStamp", DataTypes.TimestampType, true, Metadata.empty());
                index++;
                continue;
            }
            encoderCols[index] = col;
            index++;
        }
        return new StructType(encoderCols);
    }

    private Dataset<Row> windowToDateMapper(Dataset<Row> stream){
        StructType encoderSchema = windowToTimeStampStructType(stream.schema());
        return stream.map(
                (MapFunction<Row, Row>) row -> {
                    Row duration = row.getStruct(row.fieldIndex("window"));
                    Timestamp timeStamp = duration.getTimestamp(0);

                    Iterator<StructField> iterator = row.schema().iterator();
                    Object[] values = new Object[row.schema().length()];
                    int idx = 0;
                    while (iterator.hasNext()){
                        StructField next = iterator.next();
                        if(next.name().equals("window")){
                            values[idx] = timeStamp;
                            idx++;
                            continue;
                        }
                        values[idx] = row.get(row.fieldIndex(next.name()));
                        idx++;
                    }

                    return (Row) new GenericRowWithSchema(values, encoderSchema);


                },
                RowEncoder.apply(encoderSchema));
    }

    private void H2Writer(Dataset<Row> stream){
        dbSparkClient.createTableInDB(tableName, stream.schema());
        jdbcSink.setSchema(stream.schema());
        try {
            stream.writeStream()
                    .foreach(jdbcSink)
                    .outputMode("append")
                    .start()
                    .awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }


    private void PrintToConsole(Dataset<Row> stream){
        try {
            stream.writeStream()
                    .outputMode("append")
                    .format("console")
                    .start()
                    .awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }

    }
    private void createTempViewFromSocket(){
        // Dataset<Row> stream = createSocketConnection();
        // stream = convertJsonStringToObject(stream);
        // createTempView(stream);
        createTempView(withColumnTimestamp(convertJsonStringToObject(createSocketConnection())));
    }

    private Dataset<Row> createSocketConnection(){
        return spark
                .readStream()
                .format("socket")
                .option("host", host)
                .option("port", port)
                .load();
    }
    private Dataset<Row> convertJsonStringToObject(Dataset<Row> stream){
        String castToColJson = "CAST(value AS STRING) as json";
        return stream.selectExpr(castToColJson)
                .select(from_json(col("json"), schemaHandler.getSchema()).alias("tmp")).select("tmp.*");

    }

    private Dataset<Row> withColumnTimestamp(Dataset<Row> stream){
        return stream.withColumn("timeStamp", current_timestamp());
    }

    private Dataset<Row> selectColSqlExecutor(String sql){
        return spark.sql(sql);
    }

    private Dataset<Row> timeDifferenceMapper(Dataset<Row> stream){
        // encoder depends on the structure, we need to look in the schema for what the types are.
        StructField[] encoderCols = new StructField[]{
                new StructField("waitingTime", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("phoneCallDuration", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timeStamp", DataTypes.TimestampType, true, Metadata.empty())
        };
        StructType encoderSchema = new StructType(encoderCols);

        return stream.map(
                (MapFunction<Row, Row>) row -> {
                    Long enter = Long.parseLong(row.getString(row.fieldIndex("Enter")));
                    Long handling = Long.parseLong(row.getString(row.fieldIndex("Handling")));
                    Long ended = Long.parseLong(row.getString(row.fieldIndex("Ended")));


                    Double waitingTime = ((handling - enter)/1000.0)/60;
                    Double phoneCallDuration = ((ended - handling)/1000.0)/60;


                    Object[] values = new Object[]{waitingTime, phoneCallDuration,
                            row.getTimestamp(row.fieldIndex("timeStamp"))};
                    return (Row) new GenericRowWithSchema(values, encoderSchema);


                },
                RowEncoder.apply(encoderSchema));
    }
    private RelationalGroupedDataset groupByWindow(Dataset<Row> stream, String[] columnNames ) {
        Column[] cols = new Column[columnNames.length + 1];
        cols[0] = functions.window(stream.col("timeStamp"), windowSize);
        for(int i = 1; i <= columnNames.length; i++){
            cols[i] = col(columnNames[i-1]);
        }
        return stream.groupBy(cols);
    }
    private Dataset<Row> averageOfWindow(RelationalGroupedDataset groupedDataset, String[] colsToAvg) {
        return groupedDataset
                .agg(
                        expr("avg(waitingTime)").as("avg_waitingTime"),
                        expr("avg(phoneCallDuration)").as("avg_phoneCallDuration")
                );
    }

    private void createTempView(Dataset<Row> stream){
        stream.createOrReplaceTempView(tableName);
    }

    public static final class Builder {
        private SchemaHandler schemaHandler;
        private SparkSession spark;
        private String host;
        private int port;
        private String tableName;
        private String windowSize;
        private String waterMarkThreshold;
        private DBSparkClient dbSparkClient;
        private JDBCSink jdbcSink;

        private Builder() {
        }

        public static Builder aProcessor() {
            return new Builder();
        }

        public Builder withSchemaHandler(SchemaHandler schemaHandler) {
            this.schemaHandler = schemaHandler;
            return this;
        }

        public Builder withSpark(SparkSession spark) {
            this.spark = spark;
            return this;
        }

        public Builder withHost(String host) {
            this.host = host;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withWindowSize(String windowSize) {
            this.windowSize = windowSize;
            return this;
        }

        public Builder withWaterMarkThreshold(String waterMarkThreshold) {
            this.waterMarkThreshold = waterMarkThreshold;
            return this;
        }

        public Builder withDbSparkClient(DBSparkClient dbSparkClient) {
            this.dbSparkClient = dbSparkClient;
            return this;
        }

        public Builder withJdbcSink(JDBCSink jdbcSink) {
            this.jdbcSink = jdbcSink;
            return this;
        }

        public Processor build() {
            return new Processor(spark, host, port, schemaHandler, tableName, windowSize, waterMarkThreshold, dbSparkClient, jdbcSink);
        }
    }
}
