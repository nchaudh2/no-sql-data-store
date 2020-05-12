
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class Server {

    // this server details
    private int portNumber;
    private String name;
    private String ip;
    private int replicaFactor;

    // file paths
    private String nodeFilePath;
    private String logFilePath;
    private String hintedHandOffFilePath;

    // data structures
    private Map<String, ServerData> allServersData;
    private Map<Integer, ValueMetaData> keyValueDataStore;
    private Map<String, AcknowledgementToClientListener> CoordinatorAcknowledgementLog;
    private Map<String, List<ConflictingReplica>> failedWrites;

    // flag to stop print
    private static boolean printFlag = false;

    private void initServer(String[] args) {
        if (args.length > 1) {
            portNumber = Integer.parseInt(args[0]);
            nodeFilePath = args[1];
        } else {
            System.out.println("Invalid number of arguments to controller");
            System.exit(0);
        }

        FileProcessor fPro = new FileProcessor(nodeFilePath);

        allServersData = new TreeMap<>();
        keyValueDataStore = new ConcurrentSkipListMap<Integer, ValueMetaData>() {
            @Override
            public String toString() {
                StringBuilder sbr = new StringBuilder();
                sbr.append("\n\n------------------------------------------------------------\n");
                sbr.append("Key Value Store\n");
                sbr.append("------------------------------------------------------------\n");
                sbr.append(String.format("%-50s%s\n", "Key", "Value"));
                sbr.append("------------------------------------------------------------\n");
                for (int key : this.keySet()) {
                    sbr.append(String.format("%-15d%-10s\n", key, this.get(key)));
                }
                sbr.append("------------------------------------------------------------\n");
                sbr.append("------------------------------------------------------------\n");
                return sbr.toString();
            }
        };
        CoordinatorAcknowledgementLog = new ConcurrentSkipListMap<>();
        failedWrites = new ConcurrentSkipListMap<>();
        replicaFactor = 4;

        String str = "";
        while ((str = fPro.readLine()) != null) {
            ServerData nodeServerData = new ServerData(str);
            allServersData.put(nodeServerData.getName(), nodeServerData);

            if (nodeServerData.getPort() == portNumber) {
                name = nodeServerData.getName();
                ip = nodeServerData.getIp();
            }
        }
        if (allServersData.size() < replicaFactor) {
            System.out.println("Not enough servers in the file, require " + replicaFactor + " for the system");
            System.exit(1);
        }


        File dir = new File("dir");
        if (!dir.exists()) {
            dir.mkdir();
        }

        // set server's log file path
        StringBuilder builder = new StringBuilder();
        try {
            logFilePath = dir.getCanonicalPath() + "/"
                    + builder.append(name).append("LogFile.txt").toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // set Hinted Hand off log file path
        StringBuilder strBuilder = new StringBuilder();
        try {
            hintedHandOffFilePath = dir.getCanonicalPath() + "/" + strBuilder.append(name).append("hhf.txt").toString();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        // read Node.txt and write into allServerData

        // read logFile and write to keyValueDataStore
        File file = new File(logFilePath);
        if (file.exists()) {
            MyCassandra.LogBook.Builder logBook = MyCassandra.LogBook.newBuilder();

            // Read the existing Log book.
            try {
                logBook.mergeFrom(new FileInputStream(logFilePath));
            } catch (IOException ex) {
                System.out.println("Error reading log file in initServer");
                ex.printStackTrace();
            }
            print("Reading From log Start\n");

            // copy log messages with log-end flag = 1 to key-value Map
            for (MyCassandra.LogMessage log : logBook.getLogList()) {
                if (log.getLogEndFlag()) {

                    if (keyValueDataStore.containsKey(log.getKey())) {
                        // If key is present in our keyValue store then compare time-stamps
                        keyValueDataStore.get(log.getKey()).updateKeyWithValue(log.getTimeStamp(), log.getValue());
                    } else {
                        keyValueDataStore.put(log.getKey(), new ValueMetaData(log.getTimeStamp(), log.getValue()));
                    }
                    //print(keyValueDataStore.get(log.getKey()));
                }
            }

            print(keyValueDataStore);
            print("\nReading From log End");
        }

        //read hinted-hand off file and write to FailedWrites
        file = new File(hintedHandOffFilePath);
        if (file.exists()) {
            MyCassandra.HintedHandOffBook.Builder hhfBook = MyCassandra.HintedHandOffBook.newBuilder();
            try {
                hhfBook.mergeFrom(new FileInputStream(hintedHandOffFilePath));
            } catch (IOException ex) {
                System.out.println("Error reading hhf file in initServer");
                ex.printStackTrace();
            }

            //copy all failedWrite messages to failedWrites data structure
            for (MyCassandra.HintedHandOff message : hhfBook.getLogList()) {

                List<ConflictingReplica> list = new ArrayList<>();
                ConflictingReplica cr = null;
                for (MyCassandra.WrapperMessage wrapperMessage : message.getAllWrapperMessageList()) {
                    cr = new ConflictingReplica(message.getServerName(), wrapperMessage);
                    list.add(cr);
                    failedWrites.put(message.getServerName(), list);
                }
            }
        }
    }

    private static void print(Object obj) {
        if (printFlag) {
            if (obj != null)
                System.out.println(obj.toString());
            else
                System.out.println("null");
        }
    }

    //to get the replica server list
    private List<ServerData> getReplicaServersList(String keyNode) {
        String[] mapKeys = allServersData.keySet().toArray(new String[allServersData.size()]);
        int keyPosition = Arrays.asList(mapKeys).indexOf(keyNode);
        List<ServerData> nodeServerDataList = new ArrayList<>();

        for (int i = 0; i < replicaFactor && i < allServersData.size(); i++) {
            nodeServerDataList.add(
                    allServersData.get(
                            mapKeys[(allServersData.size() + keyPosition + i) % allServersData.size()]
                    ));
        }
        return nodeServerDataList;
    }

    private ServerData getPrimaryReplicaForKey(int key) {
        int num = (key + allServersData.size()) % allServersData.size();
        String[] nodeKeys = allServersData.keySet().toArray(new String[allServersData.size()]);
        return allServersData.get(nodeKeys[num]);
    }

    // coordinator processing client's read request
    private void processingClientReadRequest(MyCassandra.ClientReadRequest clientReadRequest, Socket clientSocket) {
        int key = clientReadRequest.getKey();
        if (key > 255 || key < 0) {
            sendAcknowledgementToClient(key, null, "Wrong key requested", clientSocket);
        } else {
            //String value = clientReadRequest.getValue();
            MyCassandra.ConsistencyLevel clientConsistencyLevel = clientReadRequest.getConsistencyLevel();

            ServerData primaryReplica = getPrimaryReplicaForKey(key);

            print(primaryReplica.toString());
            List<ServerData> replicaServerList = getReplicaServersList(primaryReplica.getName());
            print("The replica servers for key " + key + "\n" + replicaServerList);

            String timeStamp = getCurrentTimeString();

            CoordinatorAcknowledgementLog.put(timeStamp, new AcknowledgementToClientListener(null, clientSocket, clientConsistencyLevel, timeStamp, key, null, replicaServerList));

            int messageSentCounterToReplica = 0;
            for (ServerData replica : replicaServerList) {

                MyCassandra.GetKeyFromCoordinator.Builder getKeyFromCoordinatorBuilder = MyCassandra.GetKeyFromCoordinator.newBuilder();

                getKeyFromCoordinatorBuilder.setKey(key)
                        .setTimeStamp(timeStamp)
                        .setCoordinatorName(name);

                MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.newBuilder()
                        .setGetKeyFromCoordinator(getKeyFromCoordinatorBuilder).build();

                try {
                    sendMessageViaSocket(replica, message);
                    messageSentCounterToReplica++;
                } catch (IOException e) {
                    CoordinatorAcknowledgementLog.get(timeStamp).getReplicaAcknowledgementMap().get(replica.getName()).setDown(true);
                    System.err.println("Replica server " + replica.getName() + "is down while Co-od processing Client's Read Request....");
//                e.printStackTrace();
                }

            }
            //if message sent is less then the consistency level then send an error acknowledgement ot he client
            if (messageSentCounterToReplica < clientConsistencyLevel.getNumber()) {
                //return client an error message
                String errorMessage = "Cannot find the key " + key;
                sendAcknowledgementToClient(key, null, errorMessage, clientSocket);
                CoordinatorAcknowledgementLog.remove(timeStamp);
            }
        }
    }

    // coordinator processing client's write request
    private void processingClientWriteRequest(MyCassandra.ClientWriteRequest clientWriteRequest, Socket clientSocket) {
        int key = clientWriteRequest.getKey();
        if (key > 255 || key < 0) {
            sendAcknowledgementToClient(key, null, "Wrong key requested", clientSocket);
        } else {
            Map<ServerData, ConflictingReplica> failedWriteRequestListMap = new LinkedHashMap<>();
            String value = clientWriteRequest.getValue();
            MyCassandra.ConsistencyLevel clientConsistencyLevel = clientWriteRequest.getConsistencyLevel();

            ServerData primaryReplica = getPrimaryReplicaForKey(key);

            print(primaryReplica.toString());
            List<ServerData> replicaServerList = getReplicaServersList(primaryReplica.getName());
            print(replicaServerList);

            String timeStamp = getCurrentTimeString();
            CoordinatorAcknowledgementLog.put(timeStamp, new AcknowledgementToClientListener(null, clientSocket, clientConsistencyLevel, timeStamp, key, value, replicaServerList));


            for (ServerData replica : replicaServerList) {
                MyCassandra.WrapperMessage message = null;
                try {

                    MyCassandra.PutKeyFromCoordinator.Builder putKeyFromCoordinatorBuilder = MyCassandra.PutKeyFromCoordinator.newBuilder();

                    putKeyFromCoordinatorBuilder.setKey(key);
                    putKeyFromCoordinatorBuilder.setTimeStamp(timeStamp);
                    putKeyFromCoordinatorBuilder.setValue(value);
                    putKeyFromCoordinatorBuilder.setCoordinatorName(name);
                    putKeyFromCoordinatorBuilder.setIsReadRepair(false);


                    message = MyCassandra.WrapperMessage.newBuilder()
                            .setPutKeyFromCoordinator(putKeyFromCoordinatorBuilder).build();


                    sendMessageViaSocket(replica, message);
                } catch (IOException e) {
//                e.printStackTrace();
                    CoordinatorAcknowledgementLog.get(timeStamp).getReplicaAcknowledgementMap().get(replica.getName()).setDown(true);
                    System.err.println("Replica server " + replica.getName() + "is down while Co-od processing Client Write Request....");
                    ConflictingReplica conflictingReplica = new ConflictingReplica(replica.getName(), message);
                    failedWriteRequestListMap.put(replica, conflictingReplica);
                    //addFailedWriteRequestForReplica(replica, conflictingReplica);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            if ((replicaFactor - failedWriteRequestListMap.size()) < clientConsistencyLevel.getNumber()) {
                String errorMessage = "Cannot write the key " + key;
                sendAcknowledgementToClient(key, null, errorMessage, clientSocket);
                CoordinatorAcknowledgementLog.remove(timeStamp);
            } else {
                for (ServerData serverData : failedWriteRequestListMap.keySet()) {
                    ConflictingReplica conflictingReplica = failedWriteRequestListMap.get(serverData);
                    addFailedWriteRequestForReplica(serverData, conflictingReplica);
                }
            }
        }
    }

    private void addFailedWriteRequestForReplica(ServerData replica, ConflictingReplica conflictingReplica) {
        print("----Failed WriteRequest For Replica Start----");
        print("Hinted Hand Off Saved for : " + replica.getName());
        String serverName = replica.getName();
        if (failedWrites.containsKey(serverName)) {
            List<ConflictingReplica> messages = failedWrites.get(serverName);
            messages.add(conflictingReplica);
        } else {
            List<ConflictingReplica> messages = new ArrayList<>();
            messages.add(conflictingReplica);
            failedWrites.put(serverName, messages);
        }
        writeToFile();
        print("----Failed WriteRequest For Replica End-----");
    }

    private void writeToFile() {

        MyCassandra.HintedHandOffBook.Builder hhfBook = MyCassandra.HintedHandOffBook.newBuilder();

        for (Map.Entry<String, List<ConflictingReplica>> entry : failedWrites.entrySet()) {
            String serverName = entry.getKey();
            List<ConflictingReplica> listOfWriteMessages = entry.getValue();

            ArrayList<MyCassandra.WrapperMessage> wrapperList = new ArrayList<>();
            for (ConflictingReplica cr : listOfWriteMessages) {
                wrapperList.add(cr.getMessage());
            }

            MyCassandra.HintedHandOff hhf = MyCassandra.HintedHandOff.newBuilder().setServerName(serverName).addAllAllWrapperMessage(wrapperList).build();
            hhfBook.addLog(hhf);
        }

        FileOutputStream output;
        try {
            output = new FileOutputStream(hintedHandOffFilePath);
            hhfBook.build().writeTo(output);
            output.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("Hinted hand off file not found");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // normal server processing get key request from co-ordinator
    private void processingGetKeyFromCoordinator(MyCassandra.GetKeyFromCoordinator getKeyFromCoordinator) {
        int key = getKeyFromCoordinator.getKey();
        String timeStamp = getKeyFromCoordinator.getTimeStamp();
        String coordinatorName = getKeyFromCoordinator.getCoordinatorName();

        MyCassandra.AcknowledgementToCoordinator.Builder acknowledgementBuilder = MyCassandra.AcknowledgementToCoordinator.newBuilder();

        acknowledgementBuilder.setKey(key);
        acknowledgementBuilder.setReplicaName(name);
        acknowledgementBuilder.setRequestType(MyCassandra.RequestType.READ);
        acknowledgementBuilder.setCoordinatorTimeStamp(timeStamp);

        if (keyValueDataStore.containsKey(key)) {
            ValueMetaData data = keyValueDataStore.get(key);
            String value = data.getValue();
            String replicaTimeStamp = data.getTimeStamp();

            print(data);

            acknowledgementBuilder.setReplicaTimeStamp(replicaTimeStamp);
            acknowledgementBuilder.setValue(value);

        } else {
            acknowledgementBuilder.setErrorMessage("Key " + key + " not found ");
        }

        MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.newBuilder().setAcknowledgementToCoordinator(acknowledgementBuilder).build();

        ServerData coordinator = allServersData.get(coordinatorName);

        // send acknowledgement to coordinator
        try {
            sendMessageViaSocket(coordinator, message);
            print("Read Ack message sent to the Coordinator : " + coordinatorName + " .....");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // normal server processing putkey request from co-ordinator
    private void processingPutKeyFromCoordinatorRequest(MyCassandra.PutKeyFromCoordinator putKeyFromCoordinator) {
        int putKey = putKeyFromCoordinator.getKey();
        String putValue = putKeyFromCoordinator.getValue();
        String putTimeStamp = putKeyFromCoordinator.getTimeStamp();
        String coordinatorName = putKeyFromCoordinator.getCoordinatorName();
        // BEGIN : Nikhil's pre commite code

        MyCassandra.LogMessage logMessage = MyCassandra.LogMessage.newBuilder().setKey(putKey)
                .setValue(putValue).setTimeStamp(putTimeStamp)
                .setLogEndFlag(false).build();

        MyCassandra.LogBook.Builder logBook = MyCassandra.LogBook.newBuilder();

        // Read the existing Log book.
        try {
            logBook.mergeFrom(new FileInputStream(logFilePath));
        } catch (FileNotFoundException e) {
            System.out.println(": File not found.  Creating a new file.");
        } catch (IOException ex) {
            System.out.println("Error reading log file");
            ex.printStackTrace();
        }

        // Add a logMessage.
        logBook.addLog(logMessage);

        // Write the new log book back to disk.
        FileOutputStream output;
        try {
            output = new FileOutputStream(logFilePath);
            logBook.build().writeTo(output);
            output.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Read the existing Log book.
        logBook = MyCassandra.LogBook.newBuilder();
        try {
            logBook.mergeFrom(new FileInputStream(logFilePath));
        } catch (FileNotFoundException e) {
            System.out.println(": File not found.  Creating a new file.");
        } catch (IOException ex) {
            System.out.println("Error reading log file");
            ex.printStackTrace();
        }

        // END: Nikhil's pre commite code
        // DONE : do pre-commit processing via log file and code

        MyCassandra.LogBook.Builder newLogBook = MyCassandra.LogBook.newBuilder();
        for (MyCassandra.LogMessage log : logBook.getLogList()) {
            if (log.getLogEndFlag()) {
                newLogBook.addLog(log);
            } else {
                int logKey = log.getKey();
                String logTimeStamp = log.getTimeStamp();
                String logValue = log.getValue();

                if (keyValueDataStore.containsKey(logKey)) {
                    //If key is present in our keyValue store then compare time-stamps
                    keyValueDataStore.get(logKey).updateKeyWithValue(logTimeStamp, logValue);
                } else {
                    keyValueDataStore.put(logKey, new ValueMetaData(logTimeStamp, logValue));
                }
                // do post commit processing via log file and code
                // update log message and add to newLogBook
                MyCassandra.LogMessage newLogMessage = MyCassandra.LogMessage.newBuilder().setKey(logKey)
                        .setValue(logValue).setTimeStamp(logTimeStamp).setLogEndFlag(true).build();
                newLogBook.addLog(newLogMessage);

                print(keyValueDataStore.get(logKey));
                // Create acknowledgement and message to coordinator

                if (!putKeyFromCoordinator.getIsReadRepair()) {
                    MyCassandra.AcknowledgementToCoordinator acknowledgement = MyCassandra.AcknowledgementToCoordinator
                            .newBuilder()
                            .setKey(logKey)
                            .setReplicaTimeStamp(logTimeStamp)
                            .setCoordinatorTimeStamp(logTimeStamp)
                            .setValue(logValue)
                            .setReplicaName(name)
                            .setRequestType(MyCassandra.RequestType.WRITE).build();

                    MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage
                            .newBuilder()
                            .setAcknowledgementToCoordinator(acknowledgement)
                            .build();

                    ServerData coordinator = allServersData.get(coordinatorName);

                    // send acknowledgement to coordinator
                    try {
                        sendMessageViaSocket(coordinator, message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Write Ack message send to Coordinator : " + coordinatorName + " .....");
                }
            }

            // Write updated logbook back to disk.
            try {
                output = new FileOutputStream(logFilePath);
                newLogBook.build().writeTo(output);
                output.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    // normal server sending acknowledgement To Coordinator
    private synchronized void processingAcknowledgementToCoordinator(MyCassandra.AcknowledgementToCoordinator acknowledgementToCoordinator) {
        int key = acknowledgementToCoordinator.getKey();
        String value = acknowledgementToCoordinator.getValue();

        MyCassandra.RequestType requestType = acknowledgementToCoordinator.getRequestType();
        String replicaName = acknowledgementToCoordinator.getReplicaName();
        String replicasCoordinatorTimeStamp = acknowledgementToCoordinator.getCoordinatorTimeStamp();
        String replicaTimeStamp = acknowledgementToCoordinator.getReplicaTimeStamp();

        AcknowledgementToClientListener acknowledgement = CoordinatorAcknowledgementLog.get(replicasCoordinatorTimeStamp);

        if (null != acknowledgement) {
            MyCassandra.ConsistencyLevel consistencyLevel = acknowledgement.getRequestConsistencyLevel();
            Socket clientSocket = acknowledgement.getClientSocket();
            boolean isSentToClient = acknowledgement.isSentToClient();

            AcknowledgementData acknowledgementData = acknowledgement.getAcknowledgementDataByServerName(replicaName);

            String errorMessage = acknowledgementToCoordinator.getErrorMessage();
            if (null != acknowledgementData) {
                int replicaKey = acknowledgementData.getKey();
                String replicaValue = acknowledgementData.getValue();

                if (requestType.equals(MyCassandra.RequestType.WRITE)) {
                    if (key == replicaKey && replicaValue.equalsIgnoreCase(value)) {
                        acknowledgementData.setAcknowledge(true);
                    } else {
                        //inconsistency
                    }
                }
                //Read request
                else if (requestType.equals(MyCassandra.RequestType.READ)) {
                    if (value == null && errorMessage != null && errorMessage.trim().length() > 0) {
                        //value is null because key does not exist

                    } else {
                        acknowledgement.setTimeStampAndValueFromReplica(replicaName, replicaTimeStamp, value);
                        //set the time stamp of last write operation into Acknowledgement log
                        acknowledgementData.setAcknowledge(true);
                    }
                }
                List<String> replicaAcknowledgementList = acknowledgement.getAcknowledgedListForTimeStamp();
                int acknowledgeCount = replicaAcknowledgementList.size();

                //check
                if (requestType.equals(MyCassandra.RequestType.WRITE)) {
                    if (acknowledgeCount >= consistencyLevel.getNumber() && !isSentToClient) {
                        acknowledgement.setSentToClient(true);
                        sendAcknowledgementToClient(key, value, "Request processed successfully", clientSocket);
                        CoordinatorAcknowledgementLog.remove(replicasCoordinatorTimeStamp);
                    }

                } else if (requestType.equals(MyCassandra.RequestType.READ)) {
                    //un-comment to change time stamp of particular replica
                    //if (replicaName.equalsIgnoreCase("node2") && debugFlag && replicaTimeStamp != null && replicaTimeStamp.trim().length() > 0) {
                    //    debugFlag = false;
                    //    acknowledgement.setValueFromReplicaAcknowledgement(replicaName, value + "abhineet");
                    //    acknowledgement.setTimeStampFromReplica(replicaName, Long.toString(Long.parseLong(replicaTimeStamp) - 10000));
                    //}
                    if (acknowledgeCount >= consistencyLevel.getNumber() && !isSentToClient) {
                        String mostRecentReplicaName = replicaAcknowledgementList.get(0);
                        AcknowledgementData acknowledgeData = acknowledgement.getAcknowledgementDataByServerName(mostRecentReplicaName);
                        print("Replica with latest data : " + acknowledgeData.getReplicaName() + " Time stamp : " + acknowledgeData.getTimeStamp() + " Value : " + acknowledgeData.getValue());
                        acknowledgement.setSentToClient(true);

                        //sendAcknowledgementToClient(key, acknowledgeData.getValue(), errorMessage, clientSocket);
                        sendAcknowledgementToClient(key, acknowledgeData.getValue(), "Request processed successfully", clientSocket);
                    }
                    if (isSentToClient && acknowledgeCount == acknowledgement.getIsReplicaUpList().size()) {
                        //if (isSentToClient && acknowledgement.isInconsistent() && acknowledgeCount == acknowledgement.getIsReplicaUpList().size()) {

                        processReadRepair(acknowledgement, replicasCoordinatorTimeStamp);

                    }

                }
            }
        }
    }

    //uncomment for the above if debug test
    //boolean debugFlag = true;
    //Read repair thread
    private synchronized void processReadRepair(AcknowledgementToClientListener acknowledgement, String timeStamp) {
        Thread thread = new Thread(() -> {

            List<String> replicaAcknowledgementList = acknowledgement.getAcknowledgedListForTimeStamp();

            String mostRecentValueReplicaName = replicaAcknowledgementList.get(0);
            AcknowledgementData mostRecentValueFromReplica = acknowledgement.getAcknowledgementDataByServerName(mostRecentValueReplicaName);

            replicaAcknowledgementList.remove(mostRecentValueReplicaName);

            for (String serverName : replicaAcknowledgementList) {
                AcknowledgementData acknowledgementData = acknowledgement.getAcknowledgementDataByServerName(serverName);
                if (!mostRecentValueFromReplica.getValue().equalsIgnoreCase(acknowledgementData.getValue())) {
                    print("----Read Repair Start----");
                    MyCassandra.PutKeyFromCoordinator.Builder putKeyFromCoordinatorToConflictingReplicaBuilder = MyCassandra.PutKeyFromCoordinator.newBuilder();

                    putKeyFromCoordinatorToConflictingReplicaBuilder.setKey(mostRecentValueFromReplica.getKey())
                            .setTimeStamp(mostRecentValueFromReplica.getTimeStamp())
                            .setValue(mostRecentValueFromReplica.getValue())
                            .setCoordinatorName(mostRecentValueFromReplica.getReplicaName())
                            .setIsReadRepair(true);

                    MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage
                            .newBuilder()
                            .setPutKeyFromCoordinator(putKeyFromCoordinatorToConflictingReplicaBuilder)
                            .build();

                    ServerData replicaServer = allServersData.get(serverName);

                    try {
                        print("Read repair message sent to " + replicaServer);
                        sendMessageViaSocket(replicaServer, message);
                    } catch (IOException ex) {
//                        ex.printStackTrace();

                        ConflictingReplica conflictingServer = new ConflictingReplica(replicaServer.getName(), message);
                        addFailedWriteRequestForReplica(replicaServer, conflictingServer);
                    }
                    print("----Read Repair End------");
                }
            }
            CoordinatorAcknowledgementLog.remove(timeStamp);

        });
        thread.start();
    }

    // coordinator sending acknowledgement to client
    private void sendAcknowledgementToClient(int key, String value, String errorMessage, Socket clientSocket) {
        if (value == null)
            value = "";
        if (errorMessage == null) {
            errorMessage = "";
        }
        MyCassandra.AcknowledgementToClient acknowledgementToClient = MyCassandra.AcknowledgementToClient.newBuilder().setKey(key).setValue(value).setErrorMessage(errorMessage).build();
        MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.newBuilder().setAcknowledgementToClient(acknowledgementToClient).build();
        print("Send to client: " + message + " " + clientSocket.isClosed());
        try {
            if (clientSocket != null && !clientSocket.isClosed()) {
                print(acknowledgementToClient);
                message.writeDelimitedTo(clientSocket.getOutputStream());
                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized void processingOldReachedButNoAck(String coordinatorName) {
        //CHECK current sender (Co-ordinator) failed in past after taking read/Write request
        //From co-ordinator of that time ( receiver of now)
        try {
            for (Map.Entry<String, AcknowledgementToClientListener> e : CoordinatorAcknowledgementLog.entrySet()) {
                AcknowledgementToClientListener value = e.getValue();
                Map<String, AcknowledgementData> acknoledgedReplicas = value.getReplicaAcknowledgementMap();
                for (Map.Entry<String, AcknowledgementData> replica : acknoledgedReplicas.entrySet()) {
                    AcknowledgementData replicaAckData = replica.getValue();
                    if (replica.getKey().equals(coordinatorName) && (replicaAckData.isAcknowledge() == false) && (replicaAckData.isDown() == false) && (null != replicaAckData.getValue())) {
                        //Add that replica to write failed Q

                        MyCassandra.PutKeyFromCoordinator.Builder putKeyFromCoordinatorBuilder = MyCassandra.PutKeyFromCoordinator.newBuilder();
                        putKeyFromCoordinatorBuilder.setKey(replicaAckData.getKey());
                        putKeyFromCoordinatorBuilder.setTimeStamp(replicaAckData.getTimeStamp());
                        putKeyFromCoordinatorBuilder.setValue(replicaAckData.getValue());
                        putKeyFromCoordinatorBuilder.setCoordinatorName(name);
                        putKeyFromCoordinatorBuilder.setIsReadRepair(true);
                        MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.newBuilder()
                                .setPutKeyFromCoordinator(putKeyFromCoordinatorBuilder).build();
                        sendMessageViaSocket(allServersData.get(replicaAckData.getReplicaName()), message);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendMessageViaSocket(ServerData serverData, MyCassandra.WrapperMessage wrapperMessage) throws IOException {
        print("----Socket message Start----");
        Socket socket = new Socket(serverData.getIp(), serverData.getPort());
        wrapperMessage.writeDelimitedTo(socket.getOutputStream());
        socket.close();
        print("\n\nMessage sent via socket to " + serverData + "\nmessage \n" + wrapperMessage);
        print("----Socket message End-----");
    }

    private String getCurrentTimeString() {
        return Long.toString(System.currentTimeMillis());
    }

    private void sendPendingRequestsToCoordinator(String respawnReplica) {
        if (failedWrites.containsKey(respawnReplica)) {
            print("----Hinted hand off message Start----");
            print("Hinted Hand Off for : " + respawnReplica);
            List<ConflictingReplica> failedWritesList = failedWrites.get(respawnReplica);

            boolean allSent = true;
            for (ConflictingReplica cr : failedWritesList) {
                try {
                    sendMessageViaSocket(allServersData.get(cr.getServerName()), cr.getMessage());
                } catch (IOException e) {
                    System.out.println("could not send message to " + cr.getServerName());
//                    e.printStackTrace();
                    allSent = false;
                }
            }

            //delete the server name from failedWrites.
            if (allSent) {
                failedWrites.remove(respawnReplica);
                writeToFile();
            }
            print("----Hinted hand off message End------");
        }
    }

    public final void clearConsole() {
        try {
            for (int i = 0; i < 50; ++i) System.out.println();


        } catch (final Exception e) {
            //  Handle any exceptions.
        }finally {
            System.out.println("Server started with " + portNumber);
            System.out.println(keyValueDataStore);
        }
    }

    public static void main(String[] args) {

        Server server = new Server();


        server.initServer(args);

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(server.portNumber);
            System.out.println("Server started with " +server.portNumber);

        } catch (IOException ex) {
            System.out.println("Server socket cannot be created");
            ex.printStackTrace();
            System.exit(1);
        }

        int msgCount = 0;

        while (true) {
            Socket receiver = null;
            try {
                //Thread.sleep(500);
                receiver = serverSocket.accept();
                print("\n\n----------------------------------------");
                print("====Message received count = " + (++msgCount));
                print("----------------------------------------\n\n");
                MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.parseDelimitedFrom(receiver.getInputStream());

                print(message);
                if (message != null) {
                    //Client Read Request
                    if (message.hasClientReadRequest()) {
                        print("----ClientReadRequest Start----");
                        server.processingClientReadRequest(message.getClientReadRequest(), receiver);
                        print("----ClientReadRequest End----");
                    }
                    //Client Write Request
                    else if (message.hasClientWriteRequest()) {
                        print("----ClientWriteRequest Start----");
                        server.processingClientWriteRequest(message.getClientWriteRequest(), receiver);
                        print("----ClientWriteRequest End----");
                    }
                    //Get Key From Coordinator
                    else if (message.hasGetKeyFromCoordinator()) {
                        print("----GetKeyFromCoordinator Start----");
                        String coordinatorName = message.getGetKeyFromCoordinator().getCoordinatorName();
                        server.sendPendingRequestsToCoordinator(coordinatorName);
                        server.processingGetKeyFromCoordinator(message.getGetKeyFromCoordinator());
                        receiver.close();
                        print("----GetKeyFromCoordinator End----");
                    }
                    //Put Key From Coordinator
                    else if (message.hasPutKeyFromCoordinator()) {
                        print("----PutKeyFromCoordinator Start----");
                        String coordinatorName = message.getPutKeyFromCoordinator().getCoordinatorName();
                        server.sendPendingRequestsToCoordinator(coordinatorName);
                        server.processingPutKeyFromCoordinatorRequest(message.getPutKeyFromCoordinator());
                        receiver.close();
                        print("----PutKeyFromCoordinator End----");
                    }
                    //Acknowledgement To Coordinator
                    else if (message.hasAcknowledgementToCoordinator()) {
                        print("----Acknowledgement To Coordinator Start----");
                        server.processingAcknowledgementToCoordinator(message.getAcknowledgementToCoordinator());
                        receiver.close();
                        print("----Acknowledgement To Coordinator End----");
                    }
//                    System.out.println("-----------------------------------------------------");
//                    System.out.println("---------------KEY\tVALUE\tTIME----------------------");
//                    for (Map.Entry<Integer, ValueMetaData> keyValue : server.keyValueDataStore.entrySet()) {
//                    System.out.println("\t\t\t\t"+keyValue.getKey()+"\t"+keyValue.getValue().getValue()+"\t\t"+keyValue.getValue().getTimeStamp());
//                    }
//                    System.out.println("-----------------------------------------------------");

                }

                server.clearConsole();
            } catch (IOException e) {
                System.out.println("Error reading data from socket. Exiting main thread");
                e.printStackTrace();
                System.exit(1);
            } finally {
                print("\n\n---------------------------------------------");
                print("=====Message received End count = " + msgCount);
                print("---------------------------------------------\n\n");
            }
        }
    }
}