public class ServerData {
    private String name;
    private String ip;
    private int port;

	public ServerData(String str) {
        String[] sarr = str.split(" ");
        if (sarr.length == 3) {
            name = sarr[0];
            ip = sarr[1];
            port = Integer.parseInt(sarr[2]);
        }
    }

    public String getName() {
        return name;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "[Name : " + name + ", IP : " + ip + ", Port : " + port + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

		ServerData that = (ServerData) o;

        if (port != that.port) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + port;
        return result;
    }
}
