package generator;

import org.json.JSONObject;

import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;

public class EventGenerator {
    private Map<String, Boolean> phoneNumbs;
    private Map<String, Boolean> operators;
    private TcpServer phoneServer;
    // private TcpServer operatorServer;
    private Queue<PhoneEvent> queue = new ConcurrentLinkedQueue<>();
    private List<PhoneEvent> conversations = new CopyOnWriteArrayList<>();

    private File phoneEvents = new File("phoneEvents");
    private File operatorEvents = new File("operatorEvents");
    private FileWriter phoneEventsWriter;
    private FileWriter operatorEventsWriter;
    private int eventCounter = 0;
    private int numEvents = 1000000;


    private Random random = new Random();


    private void StartConnection(int phonePort, int OperatorPort) {
        try {
            phoneEventsWriter = new FileWriter(phoneEvents, true);
            // operatorEventsWriter = new FileWriter(operatorEvents, true);


           //  phoneServer = new TcpServer();
           //  phoneServer.ServerAcceptor(phonePort);

            // operatorServer  = new TcpServer();
            // operatorServer.ServerAcceptor(OperatorPort);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void Start() throws IOException, InterruptedException {
        StartConnection(9998, 9999);
        phoneNumbs = getFileToHash("src/main/data/1000FormattedPhoneNumbs.txt", 100) ;
        operators = getFileToHash("src/main/data/handlers.txt",  100);
        System.out.println("Connection established...");

        ExecutorService executor = Executors.newFixedThreadPool(3);


        executor.submit(this::PhoneEventGenerator);
        System.out.println("Phone Event Generator Initialized...");
        executor.submit(this::OperatorDistributor);
        System.out.println("Operator Distributor Initialized... ");
        executor.submit(this::DurationManager);
        System.out.println("Duration Manager Initialized...");


    }

    @SuppressWarnings("Duplicates")
    public void simpleStart() throws IOException, InterruptedException {
        phoneNumbs = getFileToHash("src/main/data/1000FormattedPhoneNumbs.txt", 100) ;
        int totalEvents = 10000000;
        Double eventsPRSecond = 50.0;
        Double threadSleepMillisDouble = (1/eventsPRSecond) * 1000.0;
        int threadSleepMillis = (int)Math.round(threadSleepMillisDouble);
        System.out.println("ThreadSleepMillis: " + threadSleepMillis);

        TcpServer server = new TcpServer();
        server.ServerAcceptor(9998);
        System.out.println("Connection Established");

        int boosterWait = getRandomNumberInRange(1, 5);
        int boosterDur = getRandomNumberInRange(1, 5);

        for(int i = 0; i<totalEvents; i++){
            PhoneEvent event = phoneEvent(boosterWait, boosterDur);
            sendPhoneEvent(event, server);
            Thread.sleep(threadSleepMillis);
            eventCounter++;
            if (eventCounter >= 1000){
                eventCounter = 0;
                boosterWait = getRandomNumberInRange(1, 5);
                boosterDur = getRandomNumberInRange(1, 5);
                System.out.println(boosterWait);
            }
        }

    }

    private PhoneEvent phoneEvent(int boosterWait, int boosterDur){
        Long waitingTime = getWaitingDuration().longValue() * 60 * 1000 * boosterWait ;
        Long sessionDuration = getSessionDuration().longValue() * 60 * 1000 * boosterDur;
        Long operators = Long.parseLong("500")/boosterWait;
        Long companyID = getRandomNumberInRange(1,5).longValue();
        Long CurrentTime = getCurrentTime();

        return PhoneEventBuilder.aPhoneEvent()
                .withDuration(sessionDuration)
                .withNumber(getAvailableKey(phoneNumbs))
                .withEnter(CurrentTime)
                .withHandling(CurrentTime + waitingTime)
                .withEnded(CurrentTime + waitingTime + sessionDuration)
                .withOperators(operators)
                .withCompanyID(companyID)
                .build();
    }

    private void DurationManager(){
        while(true) {
            try{
                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            PhoneEvent minDuration = getMinDuration();
            if (minDuration.getDuration() < getCurrentTime()){
                RemoveConversation(minDuration);
                setAvailableKey(phoneNumbs, minDuration.getNumber());
                setAvailableKey(operators , minDuration.getOperator());

                minDuration.setEnded(getCurrentTime());

                // sendPhoneEvent(minDuration);

                sendToPhoneStream(minDuration.getNumber(), "Ended");
                // sendToOperatorStream(minDuration.getOperator(), "Available", minDuration.getNumber());
            }

        }
    }

    private void sendPhoneEvent(PhoneEvent event, TcpServer server){
        JSONObject json = new JSONObject();
        json.put("PhoneNbr", event.getNumber());
        json.put("Enter", event.getEnter());
        json.put("Handling", event.getHandling());
        json.put("Ended", event.getEnded());
        json.put("Operators", event.getOperators());
        json.put("CompanyID", event.getCompanyID());

        server.Send(json.toString());

    }

    private PhoneEvent getMinDuration(){
        PhoneEvent minDuration = conversations
                .stream()
                .min(Comparator.comparing(PhoneEvent::getDuration))
                .orElseThrow(NoSuchElementException::new);
        return minDuration;
    }
    private void OperatorDistributor(){
        while (true){
            if (queue.peek() == null){
                continue;
            }
            PhoneEvent event = queue.poll();
            String id = getAvailableKey(operators);

            event.setOperator(id);
            event.setDuration(getCurrentTime()+ event.getDuration()*1000);
            event.setHandling(getCurrentTime());

            AddToConversations(event);

            // sendToOperatorStream(id, "Busy", event.getNumber());
            sendToPhoneStream(event.getNumber(), "Handling");


        }
    }
    private void PhoneEventGenerator() {
        while (true) {
            PhoneEvent event  =  PhoneEventBuilder.aPhoneEvent()
                    .withDuration(getSessionDuration().longValue())
                    .withNumber(getAvailableKey(phoneNumbs))
                    .withEnter(getCurrentTime())
                    .build();
            AddToQueue(event);
            sendToPhoneStream(event.getNumber(), "Enter");
            try {
                Thread.sleep(100); // 10 secs
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
    private void RemoveConversation(PhoneEvent event){
        conversations.remove(event);
    }
    private void AddToQueue(PhoneEvent event){
        Thread thread = new Thread(()-> {
            queue.add(event);
        });
        thread.start();
    }
    private void AddToConversations(PhoneEvent event){
        Thread thread = new Thread(() -> {
            conversations.add(event);
        });
        thread.start();
    }

    private void sendToPhoneStream(String number, String event){
        JSONObject json = new JSONObject();
        json.put("PhoneNbr", number);
        json.put("Event", event);
        json.put("TimeStampLong", getCurrentTime());

        try {
            phoneEventsWriter.write(json.toString() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(eventCounter-1 >= numEvents) {
            System.exit(0);
        }else {
            System.out.println(eventCounter++);
        }

        // json.put("TimeStamp", convertCurrentTimeToTimestamp(getCurrentTime()));
        // System.out.println(json.toString());
        // phoneServer.Send(json.toString());
    }
    private void sendToOperatorStream(String id, String status, String number){
        JSONObject json = new JSONObject();
        json.put("ID", id);
        json.put("Status", status);
        json.put("TimeStamp", convertCurrentTimeToTimestamp(getCurrentTime()));
        json.put("PhoneNbr", number);

        try {
            operatorEventsWriter.write(json.toString() + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // System.out.println(json.toString());
        // operatorServer.Send(json.toString());

    }
    private void setAvailableKey(Map<String, Boolean> map, String key ){
        map.replace(key,true);
    }
    private String getAvailableKey(Map<String, Boolean> map){
        Object[] keys = map.keySet().toArray();
        Object key;
        do {
            key = keys[random.nextInt(keys.length)];
        }while (!map.get(key.toString()));
        // map.put(key.toString(), false);

        return key.toString();
    }



    private Map<String, Boolean> getFileToHash(String fileName, int percentageAvailable){
        if (percentageAvailable < 0 || percentageAvailable > 100){
            throw new IndexOutOfBoundsException("PercentageAvailable is not in the percentage range.");
        }
        String line;
        Map<String, Boolean> hash = new ConcurrentHashMap<>();
        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null){
                if (getRandomNumberInRange(1,100) < percentageAvailable){
                    hash.put(line, true);
                }else {
                    hash.put(line, false);
                }
            }

        }catch (Exception e) {
            e.printStackTrace();
        }
        return hash;
    }

    private Long getCurrentTime(){
        Date date = new Date();
        Timestamp ts= new Timestamp(date.getTime());
        return ts.getTime();
    }

    private String convertCurrentTimeToTimestamp(Long currentTime){
        Timestamp ts= new Timestamp(currentTime);
        return ts.toString();
    }

    private Integer getSessionDuration(){
        return getRandomNumberInRange(1,12);
    }
    private Integer getWaitingDuration(){
        return getRandomNumberInRange(0, 5);
    }

    private Integer getRandomNumberInRange(int min, int max) {
        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }
        return random.nextInt((max-min)+1) + min;
    }
}

