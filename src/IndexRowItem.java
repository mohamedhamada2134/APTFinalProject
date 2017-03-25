
import java.util.Stack;

public class IndexRowItem {

    private String Word;
    private int UrlID;
    private String Importance;
    private int Position;
    int WordID;

    public void setWord(String word) {
        Word = word;
    }

    public void setUrlID(int urlID) {
        this.UrlID = urlID;
    }

    public void setWordID(int id) {
        this.WordID = id;
    }

    public void setImportance(String importance) {
        Importance = importance;
    }

    public void setPosition(int position) {
        Position = position;
    }

    public String getWord() {
        return Word;
    }

    public int getUrlID() {
        return UrlID;
    }

    public String getImportance() {
        return Importance;
    }

    public int getPosition() {
        return Position;
    }

    public int getWordID() {
        return WordID;
    }

    public void print() {

        System.out.println("Word  :" + Word);
        System.out.println("Importance  :" + Importance);
        System.out.println("Position  :" + Position);

    }

}
