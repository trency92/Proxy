package com.lvt4j.socketproxy;

import com.lvt4j.socketproxy.ProxyApp.IOExceptionRunnable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

import static java.nio.channels.SelectionKey.OP_CONNECT;
import static java.util.Collections.synchronizedList;

/**
 * @author LV on 2022年4月2日
 */
@Slf4j
@Service
public class ChannelConnector extends Thread implements UncaughtExceptionHandler {

    private Selector selector;

    /**
     * 待注册队列
     */
    private final List<Runnable> registerQueue = synchronizedList(new LinkedList<>());

    @PostConstruct
    public void init() throws IOException {
        init("ChannelConnector");
    }

    public void init(String name) throws IOException {
        setName(name);
        setUncaughtExceptionHandler(this);
        selector = Selector.open();
        start();
    }

    @PreDestroy
    public void destory() {
        try {
            selector.close();
            join(1000);
        } catch (Exception e) {
            log.error("channel connector close err", e);
        }
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (e instanceof ClosedSelectorException) return;
        log.error("channel connector err", e);
    }

    public void connect(SocketChannel channel, IOExceptionRunnable onConnect, Consumer<Exception> exHandler) {
        registerQueue.add(() -> {
            ConnectMeta cnn = new ConnectMeta();
            cnn.onConnect = onConnect;
            cnn.exHandler = exHandler;

            try {
                channel.register(selector, OP_CONNECT, cnn);
            } catch (Exception e) {
                exHandler.accept(e);
            }
        });
        selector.wakeup();
    }

    @Override
    public void run() {
        try {
            while (selector.isOpen()) {
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (!registerQueue.isEmpty()) registerQueue.remove(0).run();
                if (!selector.isOpen()) return;
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    connect(key);
                    key.cancel();
                }
            }
        } catch (Throwable e) {
            uncaughtException(this, e);
        }
    }

    private void connect(SelectionKey key) {
        if (!key.isConnectable()) return;
        ConnectMeta cnn = (ConnectMeta) key.attachment();
        try (SocketChannel channel = (SocketChannel) key.channel()) {
            channel.finishConnect();
            cnn.onConnect.run();
        } catch (Exception e) {
            cnn.exHandler.accept(e);
        }
    }

    private static class ConnectMeta {
        private IOExceptionRunnable onConnect;
        private Consumer<Exception> exHandler;
    }

}