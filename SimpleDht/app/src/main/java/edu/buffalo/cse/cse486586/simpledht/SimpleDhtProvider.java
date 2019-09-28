package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import android.app.DownloadManager;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    String myPortId;
    HashMap<String, String> hashPortMap = new HashMap<String, String>();
    HashMap<String, String> hashPortMapInverse = new HashMap<String, String>();
    String suc = "";
    String pred = "";
    List<String> lstFiles = new ArrayList<String>();
    String starResponse;
    boolean flag=false;

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
        lstFiles.add(filename);
        return uri;
    }

    public String returnValue(String selection) throws IOException {
        String valueResponse = "";
        if(!(selection.substring(0,1).equals("*") && selection.substring(1,6).equals(myPortId)  && flag)){
            flag=true;
            Socket socket = null;
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(suc));
            } catch (IOException e) {
                e.printStackTrace();
            }
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String msgToSend = "Contains:" + selection;
            out.println(msgToSend);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));

            valueResponse= in.readLine();


        }
        flag=false;
        return valueResponse;

    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        boolean result=false;
        int count = 0;

        String keyHash = null;
        try {
            keyHash = genHash(selection);


        Integer portNum = Integer.parseInt(myPortId) / 2;
        String port1 = String.valueOf(portNum);
        String portHash = genHash(port1);

        Integer predNum = Integer.parseInt(pred) / 2;
        String pred1 = String.valueOf(predNum);
        String predHash = genHash(pred1);

        if (myPortId.equals(suc)) {
            result=getContext().deleteFile(selection);
            lstFiles.remove(selection);
        } else if (predHash.compareTo(portHash) > 0) {

            if (keyHash.compareTo(predHash) > 0 || keyHash.compareTo(portHash) < 0) {
                result=getContext().deleteFile(selection);
                lstFiles.remove(selection);
            } else {
                count=deletefile(selection);
            }
        } else if (predHash.compareTo(keyHash) < 0 && portHash.compareTo((keyHash)) >= 0) {
            result=getContext().deleteFile(selection);
            lstFiles.remove(selection);
        } else {
            count=deletefile(selection);
        }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        if(result){
            count++;
        }
        // TODO Auto-generated method stub
        return count;
    }

    private int deletefile(String selection) {
        int count=0;
        String valueResponse= "0";
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(suc));

        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        String msgToSend = "delete:"+ selection;
        out.println(msgToSend);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));



            valueResponse = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Integer.valueOf(valueResponse);
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        try {
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String keyHash = genHash(key);

            Integer portNum = Integer.parseInt(myPortId) / 2;
            String port1 = String.valueOf(portNum);
            String portHash = genHash(port1);

            Integer predNum = Integer.parseInt(pred) / 2;
            String pred1 = String.valueOf(predNum);
            String predHash = genHash(pred1);

            if (myPortId.equals(suc)) {
                Actualinsert(uri, values);
            } else if (predHash.compareTo(portHash) > 0) {

                if (keyHash.compareTo(predHash) > 0 || keyHash.compareTo(portHash) < 0) {
                    Actualinsert(uri, values);
                } else {
                    String msg = "Inserting:" + key + ":" + value;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPortId);
                }
            } else if (predHash.compareTo(keyHash) < 0 && portHash.compareTo((keyHash)) >= 0) {
                Actualinsert(uri, values);
            } else {
                String msg = "Inserting:" + key + ":" + value;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPortId);
            }


        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPortId = myPort;

        String msg = "join" + myPortId;

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);

        } catch (IOException e) {
            e.printStackTrace();
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPortId);


        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
        String value = "";
        FileInputStream inputStream = null;
        if (selection.substring(0,1).equals("*")) {
            if(suc.equals(myPortId)){
                return query(uri,null,"@",null,null);
            }
            else {
                try {
                    mc = (MatrixCursor) query(uri,null,"@",null,null);
                    if(selection.length()==1){
                        value = returnValue(selection+myPortId);
                    }
                    else{
                        value = returnValue(selection);
                    }
                    if(value.length()>0){
                        //Key.value|key.Value...
                        String[] temp = value.split("##");

                        for(String s:temp){

                            String key = s.substring(0,s.indexOf("."));
                            String val = s.substring(s.indexOf(".")+1);
                            mc.addRow(new String[]{key,val});
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        else if (selection.equals("@")) {
            for (String filename : lstFiles) {
                value = "";
                try {
                    inputStream = getContext().openFileInput(filename);
                    int ch;
                    while ((ch = inputStream.read()) != -1)
                        value += (char) ch;

                } catch (Exception e) {
                    Log.e(TAG, "File write failed");
                }

                mc.addRow(new String[]{filename, value});
            }
        }

        else {

            if (lstFiles.contains(selection)) {

                try {
                    inputStream = getContext().openFileInput(selection);
                    int ch;
                    while ((ch = inputStream.read()) != -1)
                        value += (char) ch;

                    mc.addRow(new String[]{selection, value});
                } catch (Exception e) {
                    Log.e(TAG, "File write failed");
                }

            } else {
                try {
                    value = returnValue(selection);
                    if(value.length()>0) {
                        //Key.value|key.Value...
                            String key = value.substring(0, value.indexOf("."));
                            String val = value.substring(value.indexOf(".") + 1);
                            mc.addRow(new String[]{key, val});

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }
        // TODO Auto-generated method stub
        return mc;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        List<String> lst;
        private static final String CONTENT_AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";
        ContentValues[] cv = new ContentValues[10];

        private Uri buildUri(String scheme, String authority) {
            Uri.Builder uriBuilder = new Uri.Builder();
            uriBuilder.authority(authority);
            uriBuilder.scheme(scheme);
            return uriBuilder.build();
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            if (myPortId.equals("11108")) {
                lst = new ArrayList<String>();
            }
            String serverMsg;
            Socket clientsocket;
            while (true) {

                PrintWriter out = null;
                try {
                    clientsocket = serverSocket.accept();
                    out = new PrintWriter(clientsocket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientsocket.getInputStream()));

                    serverMsg = in.readLine();

                    if (serverMsg.startsWith("join")) {
                        String predHash = "";
                        String sucHash = "";

                        String portString = serverMsg.substring(serverMsg.indexOf("n") + 1);
                        Integer portNum = Integer.parseInt(portString) / 2;
                        String port = String.valueOf(portNum);
                        String portHash = genHash(port);
                        hashPortMap.put(portHash, portString);
                        hashPortMapInverse.put(portString, portHash);
                        lst.add(portHash);
                        Collections.sort(lst);

                        int i = lst.indexOf(portHash);

                        if (i != 0) {
                            predHash = lst.get(i - 1);
                        } else {
                            predHash = lst.get(lst.size() - 1);
                        }

                        if (i != lst.size() - 1) {
                            sucHash = lst.get(i + 1);
                        } else {
                            sucHash = lst.get(0);
                        }
                        out.println(hashPortMap.get(predHash) + "," + hashPortMap.get(sucHash));
                    } else if (serverMsg.startsWith("pred")) {

                        suc = serverMsg.substring(serverMsg.indexOf("d") + 1);

                    } else if (serverMsg.startsWith("suc")) {

                        pred = serverMsg.substring(serverMsg.indexOf("c") + 1);

                    } else if (serverMsg.startsWith("Inserting")) {

                        Uri uri = buildUri("content", CONTENT_AUTHORITY);

                        int first = serverMsg.indexOf(":");
                        int second = serverMsg.lastIndexOf(":");

                        String key = serverMsg.substring(first + 1, second);
                        String value = serverMsg.substring(second + 1);

                        cv[0] = new ContentValues();
                        cv[0].put("key", key);
                        cv[0].put("value", value);
                        getContext().getContentResolver().insert(uri, cv[0]);
                    } else if (serverMsg.startsWith("Contains")) {
                        String key = serverMsg.substring(serverMsg.indexOf(":") + 1);

                        String temp="";
                        if(key.contains("*")){
                            temp+=returnValue(key);
                            key="@";
                        }
                        if(temp.length()>0){
                            temp+="##";
                        }
                        Uri uri = buildUri("content", CONTENT_AUTHORITY);
                        MatrixCursor matrixCursor = (MatrixCursor) query(uri, null, key, null, null);
                        matrixCursor.moveToFirst();
                        while(!matrixCursor.isAfterLast()){
                            temp+=matrixCursor.getString(0)+".";
                            temp+=matrixCursor.getString(1)+"##";
                            matrixCursor.moveToNext();
                        }
                        if(temp.length()>0){
                           temp= temp.substring(0,temp.length()-2);
                        }
                        //out.println(matrixCursor.getString(1));
                        out.println(temp);
                    }
                    else if(serverMsg.startsWith("delete")) {
                        Uri uri = buildUri("content", CONTENT_AUTHORITY);
                        int count=delete(uri,serverMsg.substring(serverMsg.indexOf(":") + 1),null);
                        out.println(String.valueOf(count));
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        protected Void doInBackground(String... msgs) {

            try {
                PrintWriter out = null;
                BufferedReader in = null;
                String msgToSend = msgs[0];
                Socket socket = null;
                if (msgToSend.startsWith("join")) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));
                    out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgToSend);

                    in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));

                    String[] info = in.readLine().split(",");
                    pred = info[0];
                    suc = info[1];


                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(pred));

                    out = new PrintWriter(socket.getOutputStream(), true);
                    out.println("pred" + myPortId);


                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(suc));
                    out = new PrintWriter(socket.getOutputStream(), true);
                    out.println("suc" + myPortId);

                    socket.close();


                } else if (msgToSend.startsWith("Inserting")) {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(suc));
                    out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgToSend);
                }
                else if (msgToSend.startsWith("Star")) {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(suc));
                    out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgToSend);
                }


                socket.close();
            } catch (SocketException e) {
                suc = myPortId;
                pred = myPortId;
                e.printStackTrace();

            } catch (IOException e) {
                e.printStackTrace();
            } catch (NullPointerException ne) {
                suc = myPortId;
                pred = myPortId;
                ne.printStackTrace();

            }

            return null;
        }
    }
}
