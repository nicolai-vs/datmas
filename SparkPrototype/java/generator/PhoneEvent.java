package generator;

public class PhoneEvent {
    private String number;
    private Long duration;
    private String operator;
    private Long Enter;
    private Long Handling;
    private Long Ended;
    private Long operators;
    private Long CompanyID;


    public PhoneEvent(String number, Long duration) {
        this.number = number;
        this.duration = duration;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public Long getEnter() {
        return Enter;
    }

    public void setEnter(Long enter) {
        Enter = enter;
    }

    public Long getHandling() {
        return Handling;
    }

    public void setHandling(Long handling) {
        Handling = handling;
    }

    public Long getEnded() {
        return Ended;
    }

    public void setEnded(Long ended) {
        Ended = ended;
    }

    public Long getOperators() {
        return operators;
    }

    public void setOperators(Long operators) {
        this.operators = operators;
    }

    public Long getCompanyID() {
        return CompanyID;
    }

    public void setCompanyID(Long companyID) {
        CompanyID = companyID;
    }

}

