import java.io.*;

public class FileProcessor {

    private String filePath;
    private File file;
    private BufferedReader br;
    private int count;


    //constructor
    public FileProcessor(String path) {
        this.filePath = path;
        if (filePath.equals(null)) {

            System.err.println("File path is null");
            System.exit(1);
        }
        getInitializedFileObject();
        count = 0;
    }


    private BufferedReader getInitializedBufferedReaderObject(FileInputStream fStream) {
        return new BufferedReader(new InputStreamReader(fStream));
    }

    private void getInitializedFileObject() {
        br = null;
        String path = filePath;
        file = new File(path);
        if (FileChecker.fileChecker(path)) {
            try {
                br = getInitializedBufferedReaderObject(new FileInputStream(file));
            } catch (IOException ex) {
                if (br != null) try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();

                    System.exit(1);
                }
            }
        }
    }

    //get the one at a time from the input file
    public String readLine() {
        String strLine;
        try {
            if ((strLine = br.readLine()) != null) {
                count++;
                return strLine.trim();
            } else {
                br.close();
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Error in readLine");
            System.exit(1);
        }
        return null;
    }

    public int getCount() {
        return count;
    }

    public int countLines() throws IOException {

        InputStream is = new BufferedInputStream(new FileInputStream(filePath));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count+1;
        } finally {
            is.close();
        }
    }


    @Override
    public String toString() {
        String className = this.getClass().getName();
        String description = "This class has a method String readLine(...), which returns one line at a time from a file.";
        String str = String.format("Class : %s\nMethod toString()\nDescription : %s\nPrivate variable inputPath value is : %s\n", className, description, filePath);
        System.out.println(str);
        return str;
    }
}