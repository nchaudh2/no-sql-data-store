public class AcknowledgementData implements Comparable {

    //<editor-fold desc="Variables">
    private int key;
    private String value;
    private boolean acknowledge;
    private String timeStamp;
    private String replicaName;
    private boolean down;
    //</editor-fold>

    public AcknowledgementData(int keyI, String valueI, String timeStampI, String replicaNameI) {
        key = keyI;
        value = valueI;
        timeStamp = timeStampI;
        replicaName = replicaNameI;
    }

    public int getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStampI) {
        timeStamp = timeStampI;
    }

    public boolean isAcknowledge() {
        return acknowledge;
    }

    public void setAcknowledge(boolean acknowledge) {
        this.acknowledge = acknowledge;
    }

    public void setValue(String valueI) {
        value = valueI;
    }

    @Override
    public int compareTo(Object o) {
        return ((AcknowledgementData) o).timeStamp.compareTo(timeStamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AcknowledgementData that = (AcknowledgementData) o;

        if (key != that.key) return false;
        return timeStamp.equals(that.timeStamp);
    }

    @Override
    public int hashCode() {
        int result = key;
        result = 31 * result + timeStamp.hashCode();
        return result;
    }

    public String getReplicaName() {
        return replicaName;
    }

    public boolean isDown() {
        return down;
    }

    public void setDown(boolean down) {
        this.down = down;
    }

    @Override
    public String toString() {
        return "\n[replica name: " + replicaName + ", written value timestamp: " + timeStamp + ", key: "
                + key + ", value: " + value + ", has acknowledged: " + acknowledge + ", is down " + down + "]";
    }
}
