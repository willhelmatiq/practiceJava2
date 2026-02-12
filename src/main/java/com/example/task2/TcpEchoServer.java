package com.example.task2;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpEchoServer {

    private static final int PORT = 9000;

    public static void main(String[] args) {
        System.out.println("Echo Server started on port " + PORT);

        try (
                ServerSocket serverSocket = new ServerSocket(PORT);
                ExecutorService pool = Executors.newCachedThreadPool()
        ) {

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getRemoteSocketAddress());

                pool.submit(new ClientHandler(clientSocket));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ClientHandler implements Runnable {

        private final Socket socket;

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
            ) {

                String line;

                while ((line = reader.readLine()) != null) {
                    System.out.println("Received: " + line);

                    writer.write(line);
                    writer.newLine();
                    writer.flush();
                }

                System.out.println("Client disconnected: " + socket.getRemoteSocketAddress());
            } catch (IOException e) {
                System.out.println("Connection error on: " + socket.getRemoteSocketAddress());
            }
        }
    }
}
