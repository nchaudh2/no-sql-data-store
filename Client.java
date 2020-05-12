import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class Client {
    private static String filePath;
    private static Map<String, ServerData> nodeMap;
    private static boolean printFlag = true;// flag to stop print
    static {
        nodeMap = new TreeMap<>();
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket branchSocket = null;
        Socket clientSocket = null;
        if (args.length == 2) {
            filePath = args[0];
            System.out.println("Client Started for following servers...");
            FileProcessor fPro = new FileProcessor(filePath);
            String str = "";
            while ((str = fPro.readLine()) != null) {
                ServerData nodeServerData = new ServerData(str);
                nodeMap.put(nodeServerData.getName(), nodeServerData);
            }
            print(nodeMap);

            Scanner scanner = new Scanner(System.in);
            String requestLine = null;
            String[] reqTokens = null;
            while (true) {
                System.out.println("------------------------------------------------------------------------------------");
                System.out.println("Please enter the Read/Write request in following format\n<READ NODE-NAME KEY CONSISTENCY-LEVEL>\n<WRITE NODE-NAME KEY VALUE CONSISTENCY-LEVEL>");
                requestLine = scanner.nextLine();
                reqTokens = requestLine.split("\\s+");
                if (reqTokens[0].equalsIgnoreCase("exit")) {
                    System.exit(0);
                } else {
                    switch (reqTokens[0].toLowerCase()) {
                        case "read":
                            if(reqTokens.length!=4)
                                break;
                            if (reqTokens[3].equals("1") || reqTokens[3].equalsIgnoreCase("ONE"))
                                sendGETRequestToCoordinator(reqTokens[1], Integer.parseInt(reqTokens[2]), MyCassandra.ConsistencyLevel.ONE);
                            if (reqTokens[3].equals("2") || reqTokens[3].equalsIgnoreCase("TWO"))
                                sendGETRequestToCoordinator(reqTokens[1], Integer.parseInt(reqTokens[2]), MyCassandra.ConsistencyLevel.TWO);
                            break;
                        case "write":
                            if(reqTokens.length!=5)
                                break;
                            if (reqTokens[4].equals("1") || reqTokens[4].equalsIgnoreCase("ONE"))
                                sendGETRequestToCoordinator(reqTokens[1], Integer.parseInt(reqTokens[2]), MyCassandra.ConsistencyLevel.ONE);
                            if (reqTokens[4].equals("2") || reqTokens[4].equalsIgnoreCase("TWO"))
                                sendPUTRequestToCoordinator(reqTokens[1], Integer.parseInt(reqTokens[2]), reqTokens[3], MyCassandra.ConsistencyLevel.TWO);
                            break;
                        case "exit":
                            System.exit(0);
                    }
                }
            }
//            sendPUTRequestToCoordinator("node0", 1, "ABC", MyCassandra.ConsistencyLevel.TWO);
//            sendGETRequestToCoordinator("node0", 1, MyCassandra.ConsistencyLevel.TWO);
//            for (int i = 1; i < 256; i++)
//            {
//                sendPUTRequestToCoordinator("node"+args[1],i, "XYjZO" +i+args[1], MyCassandra.ConsistencyLevel.TWO);
//                sendGETRequestToCoordinator("node"+args[1],i,  MyCassandra.ConsistencyLevel.ONE);
//                sendGETRequestToCoordinator("node"+args[1], i+1, MyCassandra.ConsistencyLevel.TWO);
//            }
        }
    }

    public static int get(int num) {
        return ((nodeMap.size()) + num) % (nodeMap.size());
    }

    private static void sendPUTRequestToCoordinator(String node, int key, String value, MyCassandra.ConsistencyLevel consistencyLevel) {
        try {
            System.out.println("Sending request to Co-ordinator IP: " + nodeMap.get(node).getIp() + " Port:" + nodeMap.get(node).getPort());
            Socket socket = null;
            socket = new Socket(nodeMap.get(node).getIp(), nodeMap.get(node).getPort());
            MyCassandra.ClientWriteRequest.Builder putKeyVal = MyCassandra.ClientWriteRequest.newBuilder();
            putKeyVal.setKey(key).setValue(value).setConsistencyLevel(consistencyLevel).build();
            MyCassandra.WrapperMessage.Builder msg = MyCassandra.WrapperMessage.newBuilder();
            msg.setClientWriteRequest(putKeyVal).build().writeDelimitedTo(socket.getOutputStream());
            MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.parseDelimitedFrom(socket.getInputStream());
            //System.out.println("Message Received : " + message);
            printResponse(message);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void sendGETRequestToCoordinator(String node, int key, MyCassandra.ConsistencyLevel consistencyLevel) {
        try {
            System.out.println("Sending request to Co-ordinator IP: " + nodeMap.get(node).getIp() + " Port:" + nodeMap.get(node).getPort());
            Socket socket = null;
            socket = new Socket(nodeMap.get(node).getIp(), nodeMap.get(node).getPort());
            MyCassandra.ClientReadRequest.Builder getKeyVal = MyCassandra.ClientReadRequest.newBuilder();
            getKeyVal.setKey(key).setConsistencyLevel(consistencyLevel).build();
            MyCassandra.WrapperMessage.Builder msg = MyCassandra.WrapperMessage.newBuilder();
            msg.setClientReadRequest(getKeyVal).build().writeDelimitedTo(socket.getOutputStream());
            MyCassandra.WrapperMessage message = MyCassandra.WrapperMessage.parseDelimitedFrom(socket.getInputStream());
            //System.out.println("Message Received : " + message);
            printResponse(message);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printResponse(MyCassandra.WrapperMessage message) {
        System.out.println("" + message.getAcknowledgementToClient().getErrorMessage());
        if (null == message.getAcknowledgementToClient().getValue()) {
        } else {
            System.out.print("Key   : " + message.getAcknowledgementToClient().getKey());
            System.out.println("\t Value : " + message.getAcknowledgementToClient().getValue());
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
}
