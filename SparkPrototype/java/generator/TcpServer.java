package generator;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class TcpServer {
    private PrintWriter out;
    private ReentrantLock lock = new ReentrantLock();


    public PrintWriter getOut() {
        return out;
    }

    public void setOut(PrintWriter out) {
        lock.lock();
        this.out = out;
        lock.unlock();
    }
    public boolean ServerAcceptor(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        AtomicReference<Socket> socket = new AtomicReference<>(serverSocket.accept());
        setOut(new PrintWriter(socket.get().getOutputStream(), true));
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    socket.set(serverSocket.accept());
                    setOut(new PrintWriter(socket.get().getOutputStream(), true));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        });
        thread.start();
        return true;
    }
    public void Send(String json){
        Thread thread = new Thread(()-> {
            lock.lock();
            getOut().println(json);
            lock.unlock();
        });
        thread.start();
    }

}
