package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import edu.buffalo.cse.cse486586.simpledynamo.Message.MessageType;

import android.net.Uri;
import android.util.Log;

public class Utility {

	private final static Uri mUri = buildUri("content",
			"edu.buffalo.cse.cse486586.simpledynamo.provider");
	public static String SERVER_PORT = "10000";
	public static String[] NODE_LIST = { "11124", "11112", "11108", "11116",
			"11120" };

	private static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public static Uri getUri() {
		return mUri;
	}

	public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private static int getPortSequenceNumber(String port) {
		for (int i = 0; i < NODE_LIST.length; i++) {
			if (NODE_LIST[i].equals(port))
				return i;
		}
		return -1;
	}

	public static String getSuccessorPort(String port) {
		if (NODE_LIST.length == 1)
			return port;

		int seqNo = getPortSequenceNumber(port);
		if (seqNo == NODE_LIST.length - 1)
			return NODE_LIST[0]; // return first node
		else
			return NODE_LIST[seqNo + 1];
	}

	public static String getPredecessorPort(String port) {
		if (NODE_LIST.length == 1)
			return port;

		int seqNo = getPortSequenceNumber(port);
		if (seqNo == 0)
			return NODE_LIST[NODE_LIST.length - 1]; // return last node
		else
			return NODE_LIST[seqNo - 1];
	}

	public static String getMappedPort(String myPort, String key) {
		try {
			String myHashCode, predecessorHashCode, predecessorPort;
			String keyHashCode = genHash(key);

			while (true) {
				myHashCode = genHash(String
						.valueOf((Integer.parseInt(myPort)) / 2));
				predecessorPort = getPredecessorPort(myPort);
				predecessorHashCode = genHash(String.valueOf((Integer
						.parseInt(predecessorPort)) / 2));

				if (myHashCode.equals(predecessorHashCode)
						|| (keyHashCode.compareTo(myHashCode) < 0 && predecessorHashCode
								.compareTo(keyHashCode) < 0)
						|| (myHashCode.compareTo(predecessorHashCode) < 0 && (keyHashCode
								.compareTo(myHashCode) < 0 || keyHashCode
								.compareTo(predecessorHashCode) > 0))) {
					return myPort;
				} else {
					myPort = getSuccessorPort(myPort);
				}
			}

		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static int sendRequestToServer(Message msg) {
		Socket socket = null;
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2,
					2 }), Integer.parseInt(msg.getForwardingPort()));

			ObjectOutputStream outputStream = new ObjectOutputStream(
					socket.getOutputStream());

			outputStream.writeObject(msg);
			outputStream.flush();
			outputStream.close();
			socket.close();

		} catch (UnknownHostException e) {
			e.printStackTrace();

		} catch (IOException e) {
			e.printStackTrace();
			Log.e("SOCKET FAILED - " + msg.getMsgType(), msg.getKey() + " - "
					+ msg.getForwardingPort());

			// if query message fails, send message to successor
			if (msg.getMsgType().equals(MessageType.QUERY)) {
				Log.e("QUERY FAILED - " + msg.getForwardingPort(),
						"Sending msg to successor");
				msg.setForwardingPort(Utility.getSuccessorPort(msg
						.getForwardingPort()));
				sendRequestToServer(msg);
			}

			return 1; // could not establish connection
		}
		return 0;
	}
}
