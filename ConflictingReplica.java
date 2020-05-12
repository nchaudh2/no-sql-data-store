public class ConflictingReplica {
    private MyCassandra.WrapperMessage message;
	private String serverName;

	public ConflictingReplica(String serverNameI, MyCassandra.WrapperMessage messageI) {
		serverName = serverNameI;
        message = messageI;
    }
	
    public MyCassandra.WrapperMessage getMessage() {
        return message;
    }

	public String getServerName() {
		return serverName;
    }

}
