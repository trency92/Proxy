package com.lvt4j.socketproxy;

import com.google.common.primitives.Shorts;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.lvt4j.socketproxy.ProtocolService.Socks5.*;
import static org.junit.Assert.assertEquals;

/**
 * @author LV on 2022年4月2日
 */
public class Socks5ServiceTest extends BaseTest {

    private int port = availablePort();

    private Socks5Service service;

    private Config config;

    private ChannelReader reader;
    private ChannelWriter writer;
    private ChannelConnector connector;
    private ProtocolService protocolService;

    private ChannelAcceptor acceptor;

    private Socket socket;
    private InputStream in;
    private OutputStream out;

    private int serverPort = availablePort();
    private ServerSocket server;
    private Socket serverAccept;
    private InputStream acceptIn;
    private OutputStream acceptOut;


    @Before
    public void before() throws Exception {

        config = new Config();
        config.setSocks5(Collections.singletonList(port));

        acceptor = new ChannelAcceptor();
        invoke(acceptor, "init");

        reader = new ChannelReader();
        invoke(reader, "init");
        writer = new ChannelWriter();
        invoke(writer, "init");
        connector = new ChannelConnector();
        invoke(connector, "init");

        protocolService = new ProtocolService(reader, writer, connector);
        service = new Socks5Service(config, acceptor, protocolService);

        invoke(service, "init");

        socket = new Socket("127.0.0.1", port);
        in = socket.getInputStream();
        out = socket.getOutputStream();

        server = new ServerSocket(serverPort);
    }

    @After
    public void after() throws IOException {
        if (reader != null) invoke(reader, "destory");
        if (writer != null) invoke(writer, "destory");
        if (acceptor != null) invoke(acceptor, "destory");
        if (connector != null) invoke(connector, "destory");
        if (service != null) invoke(service, "destory");

        if (socket != null) socket.close();
        if (serverAccept != null) serverAccept.close();
        if (server != null) server.close();
    }

    @Test(timeout = 60000)
    @SuppressWarnings("unchecked")
    public void reload() throws Exception {
        List<Integer> socks5 = Arrays.asList(port, availablePort());
        config.setSocks5(socks5);
        invoke(service, "reloadConfig");
        Map<Integer, ?> servers = (Map<Integer, ?>) FieldUtils.readField(service, "servers", true);
        assertEquals(socks5.size(), servers.size());
        CollectionUtils.isEqualCollection(socks5, servers.keySet());

        socks5 = Collections.singletonList(availablePort());
        config.setSocks5(socks5);
        invoke(service, "reloadConfig");
        servers = (Map<Integer, ?>) FieldUtils.readField(service, "servers", true);
        assertEquals(socks5.size(), servers.size());
        CollectionUtils.isEqualCollection(socks5, servers.keySet());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void cleanIdle() throws Exception {
        config.setMaxIdleTime(1);

        trans_domain_localhost();

        Map<Integer, ?> servers = (Map<Integer, ?>) FieldUtils.readField(service, "servers", true);
        assertEquals(1, servers.size());

        Object serverMeta = servers.get(port);
        List<?> connections = (List<?>) FieldUtils.readField(serverMeta, "connections", true);
        assertEquals(1, connections.size());

        Thread.sleep(10);

        service.cleanIdle();

        connections = (List<?>) FieldUtils.readField(serverMeta, "connections", true);
        assertEquals(0, connections.size());
    }

    @Test
    public void handshake_illegal_ver() throws Exception {
        assertCnns(1);

        out.write(0);

        assertBs(NoAcc, in);

        assertCnns(0);
    }

    @Test
    public void handshake_illegal_nmethods() throws IOException {
        out.write(5);
        out.write(0);

        assertBs(NoAcc, in);
    }

    @Test
    public void handshake_no_noAuth() throws IOException {
        out.write(5);
        out.write(3);
        out.write(new byte[]{1, 2, 3});

        assertBs(NoAcc, in);
    }

    @Test
    public void target_illegal_ver() throws IOException {
        out.write(new byte[]{5, 3, 1, 2, 0});
        out.write(10);

        assertBs(Acc, in);

        assertBs(Fail, in);
    }

    @Test
    public void target_illegal_cmd() throws IOException {
        out.write(new byte[]{5, 3, 1, 2, 0});
        out.write(5);
        out.write(5);

        assertBs(Acc, in);

        assertBs(Fail, in);
    }

    @Test
    public void target_illegal_atyp() throws IOException {
        out.write(new byte[]{5, 3, 1, 2, 0});
        out.write(5);
        out.write(1);
        out.write(0);
        out.write(2);

        assertBs(Acc, in);

        assertBs(Fail, in);
    }

    @Test
    public void target_illegal_domainlen() throws IOException {
        out.write(new byte[]{5, 3, 1, 2, 0});
        out.write(5);
        out.write(1);
        out.write(0);
        out.write(2);
        out.write(0);

        assertBs(Acc, in);

        assertBs(Fail, in);
    }

    @Test
    public void trans_ipv4() throws IOException {
        out.write(new byte[]{5, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0});
        out.write(new byte[]{5, 1, 0, 1, 127, 0, 0, 1});
        out.write(Shorts.toByteArray((short) serverPort));

        trans();
    }

    @Test
    public void trans_domain_127_0_0_1() throws IOException {
        out.write(new byte[]{5, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0});
        out.write(new byte[]{5, 1, 0, 3});
        String domain = "127.0.0.1";
        byte[] domainBs = domain.getBytes();
        out.write(domainBs.length);
        out.write(domainBs);
        out.write(Shorts.toByteArray((short) serverPort));

        trans();
    }

    @Test
    public void trans_domain_localhost() throws IOException {
        out.write(new byte[]{5, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0});
        out.write(new byte[]{5, 1, 0, 3});
        String domain = "localhost";
        byte[] domainBs = domain.getBytes();
        out.write(domainBs.length);
        out.write(domainBs);
        out.write(Shorts.toByteArray((short) serverPort));

        trans();
    }

    private void trans() throws IOException {
        assertBs(Acc, in);
        assertBs(Suc, in);

        byte[] data = rand();
        out.write(data);

        serverAccept = server.accept();
        acceptIn = serverAccept.getInputStream();
        assertBs(data, acceptIn);

        acceptOut = serverAccept.getOutputStream();
        data = rand();
        acceptOut.write(data);
        assertBs(data, in);
    }

    @SuppressWarnings("unchecked")
    private void assertCnns(int expectedSize) throws Exception {
        Thread.sleep(100);

        Map<Integer, ?> servers = (Map<Integer, ?>) FieldUtils.readField(service, "servers", true);

        Object serverMeta = servers.get(port);
        List<?> connections = (List<?>) FieldUtils.readField(serverMeta, "connections", true);
        assertEquals(expectedSize, connections.size());
    }

}