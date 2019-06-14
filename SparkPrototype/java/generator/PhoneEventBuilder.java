package generator;

public final class PhoneEventBuilder {
    private String operator;
    private Long Enter;
    private Long Handling;
    private Long Ended;
    private String number;
    private Long duration;
    private Long operators;
    private Long CompanyID;

    private PhoneEventBuilder() {
    }

    public static PhoneEventBuilder aPhoneEvent() {
        return new PhoneEventBuilder();
    }

    public PhoneEventBuilder withCompanyID(Long companyID) {
        this.CompanyID = companyID;
        return this;
    }

    public PhoneEventBuilder withOperators(Long operators) {
        this.operators = operators;
        return this;
    }

    public PhoneEventBuilder withOperator(String operator) {
        this.operator = operator;
        return this;
    }

    public PhoneEventBuilder withEnter(Long Enter) {
        this.Enter = Enter;
        return this;
    }

    public PhoneEventBuilder withHandling(Long Handling) {
        this.Handling = Handling;
        return this;
    }

    public PhoneEventBuilder withEnded(Long Ended) {
        this.Ended = Ended;
        return this;
    }

    public PhoneEventBuilder withNumber(String number) {
        this.number = number;
        return this;
    }

    public PhoneEventBuilder withDuration(Long duration) {
        this.duration = duration;
        return this;
    }

    public PhoneEvent build() {
        PhoneEvent phoneEvent = new PhoneEvent(number, duration);
        phoneEvent.setOperator(operator);
        phoneEvent.setEnter(Enter);
        phoneEvent.setHandling(Handling);
        phoneEvent.setEnded(Ended);
        phoneEvent.setOperators(operators);
        phoneEvent.setCompanyID(CompanyID);
        return phoneEvent;
    }
}
