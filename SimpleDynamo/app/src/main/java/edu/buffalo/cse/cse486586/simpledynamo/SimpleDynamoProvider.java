package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.content.Context;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    String myPortId;
    List<String> lst;
    Map<String, String> portHash;
    Map<String, HashSet<String>> fileMap;
    String suc;
    String pred;
    String superPred;
    boolean recoveryFlag = false;

    private static final String CONTENT_AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";
    ContentValues[] cv = new ContentValues[10];

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    Uri uri = buildUri("content", CONTENT_AUTHORITY);

    public Uri Actualinsert(Uri uri, ContentValues values) {
        String filename = values.getAsString("key");
        String string = values.getAsString("value");

        FileOutputStream outputStream;

        try {
            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(string.getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
            Log.e(TAG, "File write failed");
        }
        return uri;
    }

    public String readValue(String key) throws IOException {

        String value = "";
        FileInputStream inputStream = null;

        inputStream = getContext().openFileInput(key);
        int ch;
        while ((ch = inputStream.read()) != -1)
            value += (char) ch;

        return value;
    }

    public String[] findLoc(String key) {

        String[] portToConnect = new String[3];

        if ((key.compareTo(lst.get(0)) <= 0) || (key.compareTo(lst.get(lst.size() - 1)) > 0)) {
            portToConnect[0] = portHash.get(lst.get(0));
            portToConnect[1] = portHash.get(lst.get(1));
            portToConnect[2] = portHash.get(lst.get(2));
        } else {
            int i = 1;
            for (i = 1; i < lst.size(); i++) {
                if (key.compareTo(lst.get(i)) <= 0) {
                    portToConnect[0] = portHash.get(lst.get(i));
                    break;
                }
            }
            portToConnect[0] = portHash.get(lst.get(i));

            i++;
            if (i > lst.size() - 1) {
                portToConnect[1] = portHash.get(lst.get(0));
                i = 0;
            } else {
                portToConnect[1] = portHash.get(lst.get(i));
            }

            i++;
            if (i > lst.size() - 1) {
                portToConnect[2] = portHash.get(lst.get(0));
            } else {
                portToConnect[2] = portHash.get(lst.get(i));
            }
        }

        return portToConnect;

    }

    public void ActualDelete(String key) {

        getContext().deleteFile(key);

        for (Map.Entry entry : fileMap.entrySet()) {
            String portName = (String) entry.getKey();
            HashSet<String> temp = (HashSet<String>) entry.getValue();

            if (temp.contains(key)) {
                temp.remove(key);
                fileMap.put(portName, temp);
                break;
            }
        }

    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        while (recoveryFlag) {
        }

        if (selection.equals("@")) {
            for (String k : this.getContext().fileList()) {
                this.getContext().deleteFile(k);
            }
        } else {

            String[] portToConnect = new String[3];
            try {
                String keyHash = genHash(selection);
                portToConnect = findLoc(keyHash);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            String msg = "Delete:" + selection + ":" + portToConnect[0] + ":" + portToConnect[1] + ":" + portToConnect[2];
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPortId);
        }

        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        while (recoveryFlag) {
        }
        String key = values.getAsString("key");
        String value = values.getAsString("value");

//        Log.e("Insert key", key);
//        Log.e("Insert value", value);
//        Log.e("Port on which inserted", myPortId);

        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String[] portToConnect = findLoc(keyHash);

        String msg = "Inserting:" + key + ":" + value + ":" + portToConnect[0] + ":" + portToConnect[1] + ":" + portToConnect[2];
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPortId);
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {
        lst = new ArrayList();
        fileMap = new HashMap<String, HashSet<String>>();
        portHash = new HashMap<String, String>();
        String[] avdNum = {"5554", "5556", "5558", "5560", "5562"};
        String[] portNum = {"11108", "11112", "11116", "11120", "11124"};
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPortId = myPort;
        String myAvdHash = "";

        //Deleteing all files when avd starts
        delete(uri, "@", null);

        recoveryFlag = true;

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);

        } catch (IOException e) {
            e.printStackTrace();
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        try {
            myAvdHash = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < avdNum.length; i++) {
            try {
                lst.add(genHash(avdNum[i]));
                portHash.put(genHash(avdNum[i]), portNum[i]);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(lst);

        int index = lst.indexOf(myAvdHash);
        int indexPred = 0;

        //Successors
        if (index != lst.size() - 1) {
            suc = portHash.get(lst.get(index + 1));
        } else {
            suc = portHash.get(lst.get(0));
        }

        //Predecessors
        if (index != 0) {
            pred = portHash.get(lst.get(index - 1));
            indexPred = index - 1;
        } else {
            pred = portHash.get(lst.get(lst.size() - 1));
            indexPred = lst.size() - 1;
        }

        if (indexPred != 0) {
            superPred = portHash.get(lst.get(indexPred - 1));
        } else {
            superPred = portHash.get(lst.get(lst.size() - 1));
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Recovery", myPortId);


//        Log.e("Self", myPortId);

//		Log.e("SuperSucc",superSuc);
//        Log.e("Pred", pred);
//        Log.e("SuperPred", superPred);
//        Log.e("Succ", suc);
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {

        while (recoveryFlag) {
        }

        MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
        String selHash = "";
        String ans = "";
        String[] portToConnect = new String[3];

        if (selection.equals("@")) {
            for (HashSet<String> temp : fileMap.values()) {
                for (String s : temp) {
                    try {
                        mc.addRow(new String[]{s, readValue(s)});
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else if (selection.equals("*")) {
            try {
                ans = new ClientTaskBlocking().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "*", myPortId).get();
                String[] ansTemp = ans.split("##");
                for (String s : ansTemp) {
                    String[] ansTemp2 = s.split("\\.");
                    mc.addRow(new String[]{ansTemp2[0], ansTemp2[1]});
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        } else {
            try {
                selHash = genHash(selection);
                portToConnect = findLoc(selHash);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            //Log.e("Key to be read", selection);
            String msg = "Query:" + selection + ":" + portToConnect[0] + ":" + portToConnect[1] + ":" + portToConnect[2];
            try {
                ans = new ClientTaskBlocking().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPortId).get();
                mc.addRow(new String[]{selection, ans});
//                Log.v("Query", ans + " for key: " + selection);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

            // TODO Auto-generated method stub

        }
        return mc;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        protected Void doInBackground(String... msgs) {

            PrintWriter out = null;
            BufferedReader in = null;
            String msgToSend = msgs[0];
            Socket socket = null;
            String val = "MoMsg";

            if (msgToSend.startsWith("Inserting")) {
                String[] msgToSendTemp = msgToSend.split(":");
                for (int i = 0; i <= 2; i++) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgToSendTemp[i + 3]));
                        socket.setSoTimeout(1000);
                        out = new PrintWriter(socket.getOutputStream(), true);
                        //Log.v("Inserting Message:",msgToSend);
                        out.println(msgToSendTemp[0] + ":" + msgToSendTemp[1] + ":" + msgToSendTemp[2] + ":" + msgToSendTemp[3]);
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        e.printStackTrace();
                    } catch (StreamCorruptedException e) {
                        e.printStackTrace();
                    } catch (SocketException e) {
                        e.printStackTrace();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NullPointerException ne) {
                        ne.printStackTrace();
                    }

                }
            }
            if (msgToSend.startsWith("Delete")) {
                String[] msgToSendTemp = msgToSend.split(":");
                for (int i = 0; i <= 2; i++) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgToSendTemp[i + 2]));
                        socket.setSoTimeout(1000);
                        out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(msgToSendTemp[0] + ":" + msgToSendTemp[1]);
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        e.printStackTrace();
                    } catch (StreamCorruptedException e) {
                        e.printStackTrace();
                    } catch (SocketException e) {
                        e.printStackTrace();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NullPointerException ne) {
                        ne.printStackTrace();
                    }
                }
            }

            if (msgToSend.startsWith("Recovery")) {

                String[] allPorts = {pred, superPred, suc};

                //Log.v("Recovery",msgToSend);

                for (int i = 0; i < 3; i++) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(allPorts[i]));
                        socket.setSoTimeout(1000);
                        in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));

                        out = new PrintWriter(socket.getOutputStream(), true);
                        if (i == 2) {
                            out.println("Recovery:" + myPortId);
                        } else {
                            out.println("Recovery:" + allPorts[i]);
                        }
                        val = in.readLine();

//                        System.out.println("RecoveryValis " + val + " Port responding " + allPorts[i]);
                        if (val != null && !val.equals(" ")) {
                            // Log.v("RecoveryVal",val);
                            String[] valTemp = val.split("##");
                            HashSet<String> keys = new HashSet<String>();
                            for (String s : valTemp) {
                                String[] temp1 = s.split("\\.");
                                keys.add(temp1[0]);
                                //Log.v("Recovery",s+" "+allPorts[i]);

                                cv[0] = new ContentValues();
                                cv[0].put("key", temp1[0]);
                                cv[0].put("value", temp1[1]);
                                Actualinsert(uri, cv[0]);
                            }
                            if (i == 2) {
                                fileMap.put(myPortId, keys);
                            } else {
                                fileMap.put(allPorts[i], keys);
                            }
                        }
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        e.printStackTrace();
                    } catch (StreamCorruptedException e) {
                        e.printStackTrace();
                    } catch (SocketException e) {
                        e.printStackTrace();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NullPointerException ne) {
                        ne.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                recoveryFlag = false;
            }

            return null;
        }
    }

    private class ClientTaskBlocking extends AsyncTask<String, String, String> {
        protected String doInBackground(String... msgs) {
            PrintWriter out = null;
            String msgToSend = msgs[0];
            Socket socket = null;
            BufferedReader in = null;
            String val = "";
            String[] ports = {"11108", "11112", "11116", "11120", "11124"};
//            Log.e("Querying Message", msgToSend);

            if (msgToSend.startsWith("Query")) {
                String[] msgToSendTemp = msgToSend.split(":");

                for (int i = 2; i < 5; i++) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgToSendTemp[i]));
                        socket.setSoTimeout(1000);
//                        Log.e("Querying to port,key", msgToSendTemp[i] + " " + msgToSendTemp[1]);
                        out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(msgToSendTemp[0] + ":" + msgToSendTemp[1]);
                        in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                        val = in.readLine();
//                        System.out.println("Key " + msgToSendTemp[1] + "Port which responded " + msgToSendTemp[i] + " QueryValis " + val);
                        socket.close();
                        if (val != null) {
                            return val;
                        }
                    } catch (SocketTimeoutException e) {
                        e.printStackTrace();
                    } catch (StreamCorruptedException e) {
                        e.printStackTrace();
                    } catch (SocketException e) {
                        e.printStackTrace();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NullPointerException ne) {
                        ne.printStackTrace();
                    }
                }
            }
            if (msgToSend.startsWith("*")) {
                for (String s : ports) {
                    try {
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(s));

                        socket.setSoTimeout(1000);
                        //Log.e("Querying *", myPortId);

                        out = new PrintWriter(socket.getOutputStream(), true);
                        out.println("*");
                        in = new BufferedReader(
                                new InputStreamReader(socket.getInputStream()));
                        String temp = in.readLine();
                        if (temp != null) {
                            val += temp;
                        }
                        socket.close();
                    } catch (SocketTimeoutException e) {
                        e.printStackTrace();
                    } catch (StreamCorruptedException e) {
                        e.printStackTrace();
                    } catch (SocketException e) {
                        e.printStackTrace();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NullPointerException ne) {
                        ne.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
            return val;

        }

    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            String serverMsg;
            Socket clientsocket;


            while (true) {
                String ansRecovery = "";
                String ansStar = "";

                PrintWriter out = null;
                try {
                    clientsocket = serverSocket.accept();
                    clientsocket.setSoTimeout(500);
                    out = new PrintWriter(clientsocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientsocket.getInputStream()));

                    serverMsg = in.readLine();


                    if (serverMsg.startsWith("Inserting")) {
                        while (recoveryFlag) {
                        }
                        Uri uri = buildUri("content", CONTENT_AUTHORITY);

                        String[] serverMsgTemp = serverMsg.split(":");

                        //Log.v("Server","Insert "+serverMsgTemp[1]);

                        String key = serverMsgTemp[1];
                        String value = serverMsgTemp[2];
                        String portFile = serverMsgTemp[3];

                        //Log.e("Inserting key on server",key);

                        cv[0] = new ContentValues();
                        cv[0].put("key", key);
                        cv[0].put("value", value);
                        Actualinsert(uri, cv[0]);

                        if (!fileMap.containsKey(portFile)) {
                            HashSet<String> temp = new HashSet<String>();
                            temp.add(key);
                            fileMap.put(portFile, temp);
                        } else {
                            HashSet<String> temp = fileMap.get(portFile);
                            temp.add(key);
                            fileMap.put(portFile, temp);
                        }
                    }

                    if (serverMsg.startsWith("Query")) {
                        while (recoveryFlag) {
                        }
                        String[] serverMsgTemp = serverMsg.split(":");
                        String value = readValue(serverMsgTemp[1]);
//                        Log.e("Current avd,key,val", myPortId + " " + serverMsgTemp[1] + " " + value);
                        out.println(value);

                    }

                    if (serverMsg.startsWith("*")) {
                        while (recoveryFlag) {
                        }
                        for (HashSet<String> temp : fileMap.values()) {
                            for (String s : temp) {
                                try {
                                    ansStar += s + "." + readValue(s) + "##";
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        out.println(ansStar);
                    }

                    if (serverMsg.startsWith("Delete")) {
                        while (recoveryFlag) {
                        }
                        String[] serverMsgTemp = serverMsg.split(":");
                        ActualDelete(serverMsgTemp[1]);

                    }

                    if (serverMsg.startsWith("Recovery")) {
                        if (recoveryFlag) {
//                            Log.e("Cur avd in recovery,ans", myPortId + " " + ansRecovery);
                            //out.println(ansRecovery);
                            out.println(" ");
                        } else {
                            String[] serverMsgTemp = serverMsg.split(":");
                            if (fileMap.containsKey(serverMsgTemp[1])) {
                                HashSet<String> temp = fileMap.get(serverMsgTemp[1]);
                                if (temp != null) {
                                    for (String s : temp) {
                                        ansRecovery += s + "." + readValue(s) + "##";
                                    }
                                }
//                                Log.e("Cur avd proper,ans", myPortId + " " + ansRecovery);
                            }
                            //Log.e("AnsRecovery",serverMsgTemp[1]+" " +ansRecovery);
                            if (ansRecovery.equals("")) {
                                out.println(" ");
                            } else {
                                out.println(ansRecovery);
                            }
                        }
                    }

                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                } catch (StreamCorruptedException e) {
                    e.printStackTrace();
                } catch (SocketException e) {
                    e.printStackTrace();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NullPointerException ne) {
                    ne.printStackTrace();
                }
            }
        }
    }
}
