
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.tartarus.snowball.ext.PorterStemmer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.lang.management.BufferPoolMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import java.util.regex.*;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Indexer {

    Vector<IndexRowItem> Items = new Vector<>(3);
    Document doc;
    String tag = "title";
    static int WordCounter = -1;
    int urlId;
    private static final Object lock2 = new Object();
    static String[] StopWords = new String[667];

    public Indexer(Document doc, int urlID) {
        this.doc = doc;
        this.urlId = urlID;

        BuildIndex();
        tag = "h1";
        BuildIndex();
        tag = "h2";
        BuildIndex();
        tag = "h3";
        BuildIndex();
        tag = "h4";
        BuildIndex();
        tag = "h5";
        BuildIndex();
        tag = "h6";
        BuildIndex();
        tag = "p";
        BuildIndex();
        tag = "th";
        BuildIndex();
        tag = "td";
        BuildIndex();
        tag = "span";
        BuildIndex();
        tag = "li";
        BuildIndex();
        dataBase();
    }

    public void dataBase() {
        Statement myStmt = null;
        try {
            myStmt = Crawler.connection.createStatement();
            for (int l = 0; l < Items.size(); l++) {
                int wid = Items.get(l).getWordID();
                String wrd = Items.get(l).getWord();
                int urlid = Items.get(l).getUrlID();
                String im = Items.get(l).getImportance();
                int pos = Items.get(l).getPosition();
                String sql = "insert into words (word_id,word,url_id,importance,position) values(" + wid + ",'" + wrd + "'," + urlid + ",'" + im + "'," + pos + ")";
                myStmt.executeUpdate(sql);
            }

        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static public void stopWords() {
        try {
            FileReader fileReader = new FileReader("stopwords.txt");
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            StringBuffer stringBuffer = new StringBuffer();
            String line;
            int i = 0;
            while ((line = bufferedReader.readLine()) != null) {
                StopWords[i] = line;
                i++;
            }
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void BuildIndex() {
        Elements title = doc.getElementsByTag(tag);
        String pConcatenated = "";
        for (Element x : title) {
            pConcatenated += x.text() + " ";
        }
        String[] parts = pConcatenated.split(" ");
        List<String> temp = new LinkedList<String>(Arrays.asList(parts));
        temp.replaceAll(String::toLowerCase);
        String myRegex = "[^a-zA-Z0-9]";
        int index = 0;
        for (String s : temp) {
            temp.set(index++, s.replaceAll(myRegex, ""));
        }
        temp.removeAll(Arrays.asList("", null));
        String[] stopWrds = StopWords;
        temp.removeAll(Arrays.asList(stopWrds));
        PorterStemmer stemmer = new PorterStemmer();
        for (int l = 0; l < temp.size(); l++) {
            String s1 = temp.get(l);
            stemmer.setCurrent(s1);
            stemmer.stem();
            s1 = stemmer.getCurrent();
            temp.set(l, s1);
        }

        for (int i = 0; i < temp.size(); i++) {

            IndexRowItem T1 = new IndexRowItem();
            T1.setWord(temp.get(i));
            T1.setImportance(tag);
            T1.setPosition(i);
            T1.setUrlID(urlId);
            synchronized (lock2) {
                WordCounter++;
                T1.setWordID(WordCounter);
            }
            Items.add(T1);

        }

    }

}
