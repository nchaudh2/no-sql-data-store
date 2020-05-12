public class ValueMetaData {
    private String value;
    private String timeStamp;

    public ValueMetaData(String timeStampI, String valueI) {
        timeStamp = timeStampI;
        value = valueI;
    }

    public String getValue() {
        return value;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

	// compares incomming timestamp with stored timestamp
    public void updateKeyWithValue(String timeStampI, String valueI) {
        if (timeStampI.compareTo(getTimeStamp()) >= 0) {
            value = valueI;
            timeStamp = timeStampI;
        }
    }


    @Override
    public String toString() {
        return " [Time Stamp : " + getTimeStamp() + ", Value : " + getValue() + "]";
    }


}


