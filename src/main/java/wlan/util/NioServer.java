package wlan.util;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

public class NioServer implements IoHandler {
    private static Logger log = LoggerFactory.getLogger(NioServer.class);
    private final int port;
    private final Listener listener;
    private IoAcceptor acceptor;

    public NioServer(int port, Listener listener) {
        this.port = port;
        this.listener = listener;
    }

    public void start() throws IOException {
        if (acceptor != null) {
            log.info("Server already started, please stop it first.");
            return;
        }
        acceptor = new NioSocketAcceptor();
        acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(Charset.forName("UTF-8"))));
        acceptor.setHandler(this);
        acceptor.getSessionConfig().setReadBufferSize(2048);
        acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
        acceptor.bind(new InetSocketAddress(port));
    }

    public void stop() throws IOException {
        if (acceptor != null) {
            acceptor.unbind();
            acceptor = null;
            return;
        }
    }

    @Override
    public void sessionCreated(IoSession session) throws Exception {
    }

    @Override
    public void sessionOpened(IoSession session) throws Exception {
    }

    @Override
    public void sessionClosed(IoSession session) throws Exception {
    }

    @Override
    public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
    }

    @Override
    public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
    }

    @Override
    public void messageReceived(IoSession session, Object message) throws Exception {
        if (message instanceof String) {
            this.listener.messageReceived((String) message);
        }

    }

    @Override
    public void messageSent(IoSession session, Object message) throws Exception {
    }

    public static void main(String[] args) throws IOException {
    }

    public static interface Listener {
        void messageReceived(String message) throws Exception;
    }
}
