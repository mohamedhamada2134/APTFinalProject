
import java.util.Scanner;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author mostafa4
 */
public class Main {

    static int Threads_Number;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.println("please enter the number of the Threads :");
        Threads_Number = in.nextInt();
        Indexer.stopWords();
        for (int i = 0; i < Threads_Number; i++) {
            new Crawler(Threads_Number, i).start();
        }

    }

}
