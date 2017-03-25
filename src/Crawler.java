/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author mostafa
 */
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.sql.*;
import static com.sun.org.apache.xalan.internal.lib.ExsltDynamic.map;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import static jdk.nashorn.internal.objects.NativeArray.map;
import static jdk.nashorn.internal.objects.NativeDebug.map;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.text.Normalizer.Form;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Vector;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map.Entry;
import org.jsoup.Connection.Response;
import java.lang.ThreadLocal;

public class Crawler extends Thread {

    //static
    private static final Object lock = new Object();
    private static final String USERNAME = "root";
    private static final String PASSWORD = "1234";
    private static final String CONN_STRING = "jdbc:mysql://localhost:3306/MY_DB";
    static Map<String, Boolean> CrawlerMap = new LinkedHashMap<String, Boolean>();
    static int UrlId = 0;
    static int max = 5000;
    static boolean seedsNotFinished = true;
    static Connection connection;
    static Map<String, Document> documents = new LinkedHashMap<String, Document>();
    String NextUrl;
    Statement stat2;
    boolean forall;
    boolean ChangeUrl = false;
    Vector disallowed = new Vector(3);
    Statement stat;
    String Tester;
    boolean Start;
    public int Threads_Number;
    public int Thread_ID;
    boolean choice;

    public Crawler(int threads, int threadId) {
        this.Threads_Number = threads;
        this.Thread_ID = threadId;
    }

    @Override
    public void run() {

        if (Thread_ID == 0) {
            try {
                Scanner in = new Scanner(System.in);
                System.setProperty("http.proxyHost", "192.168.5.1");
                System.setProperty("http.proxyPort", "1080");
                OpenConnection();
                DatabaseMetaData dbm = connection.getMetaData();
                ResultSet table1 = dbm.getTables(null, null, "URLS", null);
                ResultSet table2 = dbm.getTables(null, null, "words", null);
                System.out.println("Enter 1 if you want to continue previous state or 0 to start over"); 
                if (in.nextInt() == 1) {
                    choice = true;
                }
                if (table1.next() && table2.next() && choice) {
                    System.out.println("Loading State ...");
                    String sql = "Select * from URLS where Crawled=?";
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setBoolean(1, false);
                    ResultSet tableurls = ps.executeQuery();
                    while (tableurls.next()) {
                        CrawlerMap.putIfAbsent(tableurls.getString("url"), false);
                        UrlId = tableurls.getInt("urlid");
                    }
                    ps.close();
                    System.out.println("Previous State loaded.");
                    synchronized (lock) {
                        gettingNextUrl();
                    }
                    incrementUrl();
                    System.out.println(UrlId);
                    seedsNotFinished = false;
                    Tester = NextUrl;
                    Start = true;
                    Crawling();

                } else {
                    ResultSet table3 = dbm.getTables(null, null, "URLS", null);
                    ResultSet table4 = dbm.getTables(null, null, "words", null);
                    if (table3.next()) {
                        stat.executeUpdate("Drop Table URLS");
                        stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(20000), Crawled boolean, rank INT, title VARCHAR(200), description VARCHAR(200))");
                    } else {
                        stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(20000), Crawled boolean, rank INT, title VARCHAR(200), description VARCHAR(200))");
                    }
                    if (table4.next()) {
                        stat.executeUpdate("Drop Table words");
                        stat.executeUpdate("CREATE TABLE words (word_id INT,word VARCHAR(535),url_id INT ,importance VARCHAR(25),position INT)");
                    } else {
                        stat.executeUpdate("CREATE TABLE words (word_id INT,word VARCHAR(535),url_id INT ,importance VARCHAR(25),position INT)");
                    }
                    putSeeds();
                    synchronized (lock) {
                        gettingNextUrl();
                    }
                    Tester = NextUrl;
                    Start = true;
                    Crawling();
                }

            } catch (SQLException ex) {
                Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            try {

                while (CrawlerMap.size() < Threads_Number || seedsNotFinished) {
                    Thread.sleep(1);

                }

            } catch (InterruptedException ex) {
                Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Thread " + Thread_ID + " connected.");
            Start = true;
            synchronized (lock) {
                gettingNextUrl();
            }
            Tester = NextUrl;
            Crawling();

        }
    }

    public void putSeeds() {
        System.out.println("intializing seeds ....");
        String seeds[] = {"https://en.wikipedia.org/", "http://dmoztools.net/", "https://myspace.com/", "http://edition.cnn.com/", "https://www.nytimes.com/", "http://www.wikia.com/fandom", "http://www.msn.com/en-us", "http://www.encyclopedia.com/"};
        for (int i = 0; i < seeds.length; i++) {
            if (saveDocument(seeds[i], true, UrlId)) {
                saveUrl(seeds[i], UrlId, true);
            }

        }
        seedsNotFinished = false;
        System.out.println("seeds intialized successfully.");

    }

    public void OpenConnection() {
        try {
            connection = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
            System.out.println("Database connected!");
            stat = connection.createStatement();

        } catch (SQLException ex2) {
            throw new IllegalStateException("Can't Open Connection", ex2);
        }
    }

    public boolean saveDocument(String url, boolean check1, int id) {

        try {
            if (!documents.containsKey(url) && check1) {
                Document document;
                document = Jsoup.connect(url).userAgent("Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.46 Safari/536.5")
                        .timeout(1000)
                        .ignoreHttpErrors(true).ignoreContentType(true).get();
                //System.out.println("Saving the Document...");
                new Indexer(document, id);
                documents.putIfAbsent(url, document);
                //System.out.println("Documnet Saved.");
                return true;
            }
        } catch (IOException ex) {
            // Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Can't connect to the current Url.");
        }
        return false;
    }

    public void incrementUrl() {
        UrlId++;
    }

    public void saveUrl(String url, int urlid, boolean check1) {
        try {
            if (!CrawlerMap.containsKey(url) && check1) {
                CrawlerMap.putIfAbsent(url, false);
                //System.out.println(url);
                //System.out.println(Thread_ID);
                String query = "INSERT INTO URLS (urlid,url,Crawled) VALUES(?,?,?);";
                PreparedStatement preparedStmt = connection.prepareStatement(query);
                preparedStmt.setInt(1, urlid);
                preparedStmt.setString(2, url);
                preparedStmt.setBoolean(3, false);
                preparedStmt.execute();
                //System.out.println(UrlId);
                incrementUrl();
            }
        } catch (SQLException ex) {
            // Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error at inserting data into URLS table in database.");
        }
    }

    public void markAsCrawled(String url) {
        try {

            String sql = "Update URLS set Crawled=? where url=?";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setBoolean(1, true);
            ps.setString(2, url);
            ps.executeUpdate();
            ps.close();
            CrawlerMap.replace(url, true);
            // System.out.println("page " + url + " is setted as crawled from Thread " + Thread_ID);

        } catch (SQLException ex) {
            // Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in updating the database.");
        }
    }

    public void Crawling() {

        while (UrlId < max) {
            try {
                Document document_Intial = Jsoup.connect(NextUrl).userAgent("Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.46 Safari/536.5")
                        .timeout(1000)
                        .ignoreHttpErrors(true).ignoreContentType(true).get();

                org.jsoup.select.Elements links = document_Intial.select("a");
                Robots(NextUrl);
                if ((links.isEmpty())) {
                    synchronized (lock) {
                        gettingNextUrl();
                    }
                }
                for (Element e : links) {
                    if (UrlId > max) {
                        break;
                    }
                    String url = e.attr("abs:href");
                    boolean Check1 = true;
                    for (int i = 0; i < disallowed.size(); i++) {

                        if (url.contains((String) disallowed.elementAt(i))) {
                            Check1 = false;
                        }

                    }
                    if (url == "") {
                        continue;
                    }
                    if (saveDocument(url, Check1, UrlId)) {
                        saveUrl(url, UrlId, Check1);
                    }

                }
            } catch (IOException ex) {
                //Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println("Can't connect to the current Url.");
            } catch (Exception e) {

            }

            synchronized (lock) {
                gettingNextUrl();
            }
        }

    }

    public void gettingNextUrl() {
        for (Entry<String, Boolean> entry : CrawlerMap.entrySet()) {

            if (entry.getValue() == false) {

                NextUrl = entry.getKey();
                System.out.println(NextUrl);
                markAsCrawled(NextUrl);
                break;
            }

        }

    }

    public void CloseConnection() {
        try {
            connection.close();
        } catch (SQLException ex2) {
            throw new IllegalStateException("Cannot Close connection", ex2);
        }
    }

    public void Robots(String NextUrl) {

        URL aURL = null;
        try {
            aURL = new URL(NextUrl);
        } catch (MalformedURLException e) {

        }
        String t1 = aURL.getProtocol() + "://" + aURL.getHost();

        if (!Tester.equals(t1) || Start) {
            Tester = t1;
            Start = false;

            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(new URL(Tester + "/robots.txt").openStream()))) {
                String line = null;
                disallowed.clear();
                while ((line = in.readLine()) != null) {

                    if (line.contains("User-agent: *")) {

                        while ((line = in.readLine()) != null && (!line.contains("User-agent: *"))) {

                            if (line.equals("Disallow: /")) {
                                disallowed.addElement(Tester);
                            }

                            if (line.contains("Disallow:")) {

                                String[] parts = line.split(" ");

                                disallowed.addElement(parts[1]);

                            }

                        }
                    }

                }

            } catch (IOException e) {
                //e.printStackTrace();
                System.out.println("There is no Robots.txt for a website");
            }

        }

    }
}
