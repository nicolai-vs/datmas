package StreamProcessor;

import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;

import java.util.Iterator;

public class SchemaHandler {
    private StructType schema;
    public SchemaHandler(String sample){
        this.schema = sampleToSchema(sample);
    }
    public SchemaHandler(StructType schema){
        this.schema = schema;
    }

    private StructType sampleToSchema(String sample){
        JSONObject object = new JSONObject(sample);
        Iterator<String> keys = object.keys();
        StructType sc = new StructType();
        while(keys.hasNext()){
            String key = keys.next();
            sc = sc.add(key, getSampleColumnType(object.get(key).toString()));
        }
        return sc;
    }
    private String getSampleColumnType(String colValue){
        return "string";
    }

    private  boolean isNumeric(String value){
        boolean numeric = true;
        try{
            Long.parseLong(value);
        }catch (NumberFormatException e){
            numeric = false;
        }
        return numeric;
    }

    public StructType getSchema() {
        return schema;
    }

    public void setSchema(StructType schema) {
        this.schema = schema;
    }
}
