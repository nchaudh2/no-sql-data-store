import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class AcknowledgementToClientListener {
    private String clientName;
    private Socket clientSocket;
    private MyCassandra.ConsistencyLevel requestConsistencyLevel;
    private boolean isSentToClient;
    private Map<String, AcknowledgementData> replicaAcknowledgementMap;

    public AcknowledgementToClientListener(String clientNameI, Socket clientSocketI,
                                           MyCassandra.ConsistencyLevel consistencyLevelI, String timeStampI, int keyI, String valueI,
                                           List<ServerData> nodeServerDataList) {
        clientName = clientNameI;
        clientSocket = clientSocketI;
        requestConsistencyLevel = consistencyLevelI;
        replicaAcknowledgementMap = new ConcurrentSkipListMap<>();
        for (ServerData node : nodeServerDataList)
            replicaAcknowledgementMap.put(node.getName(), new AcknowledgementData(keyI, valueI, timeStampI, node.getName()));
    }

    public String getClientName() {
        return clientName;
    }

    public Socket getClientSocket() {
        return clientSocket;
    }

    public Map<String, AcknowledgementData> getReplicaAcknowledgementMap() {
        return replicaAcknowledgementMap;
    }

    public synchronized AcknowledgementData getAcknowledgementDataByServerName(String nodeName) {
        return replicaAcknowledgementMap.get(nodeName);
    }

    public synchronized List<String> getAcknowledgedListForTimeStamp() {
        List<AcknowledgementData> AcknowledgementDataList = new ArrayList<>();
        for (String name : replicaAcknowledgementMap.keySet()) {
            AcknowledgementData data = replicaAcknowledgementMap.get(name);
            if (data.isAcknowledge())
                AcknowledgementDataList.add(data);
        }
        Collections.sort(AcknowledgementDataList);

        List<String> list = new ArrayList<>();
        for (AcknowledgementData acknowledgementData : AcknowledgementDataList) {
            list.add(acknowledgementData.getReplicaName());
        }

        return list;
    }

    public MyCassandra.ConsistencyLevel getRequestConsistencyLevel() {
        return requestConsistencyLevel;
    }

    public boolean isSentToClient() {
        return isSentToClient;
    }

    public void setSentToClient(boolean sentToClient) {
        isSentToClient = sentToClient;
    }

    public synchronized void setTimeStampAndValueFromReplica(String replicaName, String timeStamp, String value) {
        AcknowledgementData acknowledgementData = getAcknowledgementDataByServerName(replicaName);
        acknowledgementData.setTimeStamp(timeStamp);
        acknowledgementData.setValue(value);
    }

    public synchronized boolean isInconsistent() {
        Set<String> set = new HashSet<>();
        for (String replicaName : getAcknowledgedListForTimeStamp()) {
            AcknowledgementData data = replicaAcknowledgementMap.get(replicaName);
            set.add(data.getValue());
        }
        return set.size() != 1;
    }

    public synchronized List<String> getIsReplicaUpList() {
        List<AcknowledgementData> replicaList = new ArrayList<>();
        for (String name : replicaAcknowledgementMap.keySet()) {
            AcknowledgementData data = replicaAcknowledgementMap.get(name);
            if (!data.isDown())
                replicaList.add(data);
        }
        Collections.sort(replicaList);

        List<String> list = new ArrayList<>();
        for (AcknowledgementData replica : replicaList) {
            list.add(replica.getReplicaName());
        }
        //Collections.reverse(list);
        return list;
    }

    @Override
    public String toString() {
        StringBuilder sbr = new StringBuilder();
        for (String str : replicaAcknowledgementMap.keySet()) {
            sbr.append(replicaAcknowledgementMap.get(str));
        }
        return String.format(
                " : \n" +
                        "request ConsistencyLevel: " + requestConsistencyLevel +
                        "\nrequest is sent to client: " + isSentToClient +
                        sbr +
                        "\n\n");
    }
}


