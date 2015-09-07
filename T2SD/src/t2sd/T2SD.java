/*
 * Caroline Pessoa Caetano - 408247
 * Henrique Squinello      - 408352
 * Trabalho 2 - Sistemas Distribuídos
 */

package t2sd;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;

/**
 *
 * @author Carol pc
 */
public class T2SD {

    /**
     * @param args the command line arguments
     */
     //Define the number of threads
    private final static int THREAD_NUMBER = 5;
    
    public static void main(String[] args) {
        // TODO code application logic here
     
        ArrayList<SDThread> threadPool = new ArrayList<SDThread>();
        ArrayList<DatagramSocket> socketList = new ArrayList<DatagramSocket>();

        //Create one socket for each thread
        for (int i = 0; i < THREAD_NUMBER; i++) {
            try {
                //Bind the socket with the local address, set port and timeout
                InetAddress addr = InetAddress.getByName("127.0.0.1");
                DatagramSocket s = new DatagramSocket(20000 + i, addr);
                s.setSoTimeout(5000);
                socketList.add(i, s);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //Create threads
        for (int i = 0; i < THREAD_NUMBER; i++) {
            SDThread t = new SDThread(i, socketList,true);
            threadPool.add(t);
        }

        //Start all of them
        for (SDThread t : threadPool) {
            t.start();
        }
    }

}
