package StreamProcessor;

import java.util.HashMap;
import java.util.Map;

public class CalculationProvider {


    public static String[] selectExprProvider(){
        String waitingTimeName = "waitingTime";
        String waitingTimeCalculation =  "(((Handling - Enter)/1000)/60)";
        String waitingTimeSelectExpr = waitingTimeCalculation + " as " + waitingTimeName;

        String durationName = "Duration";
        String durationCalculator = "(((Ended - Handling)/1000)/60)";
        String durationSelectExpr = durationCalculator + " as " + durationName;

        return new String[]{"*", waitingTimeSelectExpr, durationSelectExpr};

    }

    public static Map<String, String> aggregationMapProvider() {
        String waitingTimeName = "waitingTime";
        String durationName = "Duration";
        String operatorsName = "Operators";
        Map<String, String> map = new HashMap<>();
        map.put(waitingTimeName, "avg");
        map.put(durationName, "avg");
        map.put(operatorsName, "min");
        return map;
    }

    public static String[] columnsToGroupByProvider(){
        return new String[]{"CompanyID"};
    }
    private static Map<String, String> calculationsToMap(String[] cols, String calculation){
        Map<String, String> map = new HashMap<>();
        for (String col : cols){
            map.put(col, calculation);
        }
        return  map;
    }
}
