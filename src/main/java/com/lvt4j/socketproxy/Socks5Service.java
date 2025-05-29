package com.lvt4j.socketproxy;

import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.info.Info.Builder;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

import static com.lvt4j.socketproxy.ProxyApp.*;
import static java.util.stream.Collectors.*;

/**
 * socks5协议代理
 *
 * @author LV on 2022年3月22日
 * @author LV on 2022年4月2日
 * @see <a href="https://wiyi.org/socks5-protocol-in-deep.html">参考</a>
 * @see <a href="https://wiyi.org/socks5-implementation.html">参考</a>
 */
@Slf4j
@Service
public class Socks5Service implements InfoContributor {

    private final Config config;
    private final ChannelAcceptor acceptor;
    private final ProtocolService protocolService;

    private final Map<Integer, ServerMeta> servers = new HashMap<>();

    public Socks5Service(Config config, ChannelAcceptor acceptor, ProtocolService protocolService) {
        this.config = config;
        this.acceptor = acceptor;
        this.protocolService = protocolService;
    }

    @PostConstruct
    private void init() throws IOException {
        Config.changeCallback_socks5 = this::reloadConfig;

        reloadConfig();
    }

    private synchronized void reloadConfig() {
        List<Integer> socks5 = config.getSocks5();
        ImmutableSet.copyOf(servers.keySet()).stream().filter(p -> !socks5.contains(p)).forEach(removed -> {
            ServerMeta s = servers.remove(removed);
            if (s != null) s.destory();
        });
        for (int port : socks5) {
            if (servers.containsKey(port)) continue;
            try {
                servers.put(port, new ServerMeta(port));
                log.info("{} socks5代理启动", port);
            } catch (Exception e) {
                log.error("{} socks5代理启动失败", port, e);
            }
        }
    }

    @PreDestroy
    private synchronized void destory() throws IOException {
        Config.changeCallback_socks5 = null;
        ImmutableSet.copyOf(servers.values()).forEach(ServerMeta::destory);
    }

    @Scheduled(cron = "0/10 * * * * ?")
    public synchronized void cleanIdle() {
        servers.values().forEach(ServerMeta::cleanIdle);
    }

    @Override
    public void contribute(Builder builder) {
        if (servers.isEmpty()) return;
        builder.withDetail("socks5", config.getSocks5().stream().map(servers::get).filter(Objects::nonNull)
                .map(ServerMeta::info).collect(joining("\n")));
    }


    private class ServerMeta {

        private final int port;
        private final ServerSocketChannel serverSocketChannel;

        private final ChannelTransmitter client2target;
        private final ChannelTransmitter target2client;

        private final List<ConnectMeta> connections = Collections.synchronizedList(new LinkedList<>());

        public ServerMeta(int port) throws IOException {
            this.port = port;

            try {
                serverSocketChannel = ProxyApp.server(null, port);

                client2target = new ChannelTransmitter(port + " c->t");
                target2client = new ChannelTransmitter(port + " t->c");

                acceptor.accept(serverSocketChannel, this::accept, e -> log.error("establish connection err", e));
            } catch (Exception e) {
                destory();
                throw e;
            }
        }

        private void accept(SocketChannel client) throws IOException {
            connections.add(new ConnectMeta(client));
        }

        public void destory() {
            ImmutableSet.copyOf(connections).forEach(ConnectMeta::destory);
            if (client2target != null) client2target.destory();
            if (target2client != null) target2client.destory();
            ProxyApp.close(serverSocketChannel);
            acceptor.waitDeregister(serverSocketChannel);
            servers.remove(port);
            log.info("{} socks5代理停止", port);
        }

        public void cleanIdle() {
            synchronized (connections) {
                for (ConnectMeta connect : ImmutableSet.copyOf(connections)) {
                    if (System.currentTimeMillis() - connect.latestTouchTime < config.getMaxIdleTime()) continue;
                    connect.destory();
                }
            }
        }

        public String info() {
            List<Object> infos = new LinkedList<>();
            infos.add(port);
            connections.stream().collect(groupingBy(c -> c.targetStr, TreeMap::new, mapping(c -> c.direction, toList())))
                    .forEach((t, cs) -> {
                        infos.add("  " + t);
                        cs.forEach(cnn -> infos.add("  - " + cnn));
                    });
            return StringUtils.join(infos, "\n");
        }

        private class ConnectMeta {
            private final SocketChannel client;

            private SocketChannel target;
            private String targetStr;

            private String direction;

            private long latestTouchTime = System.currentTimeMillis();

            public ConnectMeta(SocketChannel client) throws IOException {
                this.client = client;

                try {
                    client.configureBlocking(false);

                    targetStr = "initializing";
                    direction = String.format("%s->%s->%s->%s"
                            , format(client.getRemoteAddress()), port(client.getLocalAddress())
                            , "initializing", "initializing");

                    if (log.isTraceEnabled()) log.trace("{} connecting {}", port, direction);

                    protocolService.socks5_server_connect(client, target -> {
                        ConnectMeta.this.target = target;
                        targetStr = format(target.getRemoteAddress());

                        direction = String.format("%s->%s->%s->%s"
                                , format(client.getRemoteAddress()), port(client.getLocalAddress())
                                , port(target.getLocalAddress()), format(target.getRemoteAddress()));

                        client2target.transmit(client, target, 1024, this::onTrans, this::onException);
                        target2client.transmit(target, client, 1024, this::onTrans, this::onException);

                        log.info("{} connected {}", port, direction);
                    }, this::onException);
                } catch (IOException e) {
                    destory();
                    throw e;
                }

            }

            private void onTrans() {
                latestTouchTime = System.currentTimeMillis();
            }

            private synchronized void onException(Exception e) {
                if (!isCloseException(e)) log.error("connection {} err", direction, e);
                destory();
            }

            private void destory() {
                ProxyApp.close(client);
                ProxyApp.close(target);
                connections.remove(this);

                log.info("{} disconnected {}", port, direction);
            }
        }
    }
}
