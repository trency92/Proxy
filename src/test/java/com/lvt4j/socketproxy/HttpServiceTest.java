package com.lvt4j.socketproxy;

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

import static com.lvt4j.socketproxy.ProtocolService.Http.EstablishedHeaders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author LV on 2022年4月3日
 */
public class HttpServiceTest extends BaseTest {

    private int port;

    private HttpService service;

    private Config config;
    private ChannelReader reader;
    private ChannelWriter writer;
    private ChannelAcceptor acceptor;
    private ChannelConnector connector;
    private ProtocolService protocolService;

    private Socket socket;
    private InputStream in;
    private OutputStream out;

    private int serverPort;
    private ServerSocket server;
    private Socket serverAccept;
    private InputStream acceptIn;
    private OutputStream acceptOut;


    @Before
    public void before() throws Exception {
        port = availablePort();

        config = new Config();
        config.setHttp(Collections.singletonList(port));

        acceptor = new ChannelAcceptor();
        invoke(acceptor, "init");

        reader = new ChannelReader();
        invoke(reader, "init");
        writer = new ChannelWriter();
        invoke(writer, "init");
        connector = new ChannelConnector();
        invoke(connector, "init");
        protocolService = new ProtocolService(reader, writer, connector);
        service = new HttpService(config, acceptor, protocolService);

        invoke(service, "init");

        socket = new Socket("127.0.0.1", port);
        in = socket.getInputStream();
        out = socket.getOutputStream();

        serverPort = availablePort();
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
        List<Integer> http = Arrays.asList(port, availablePort());
        config.setHttp(http);
        invoke(service, "reloadConfig");
        Map<Integer, ?> servers = (Map<Integer, ?>) FieldUtils.readField(service, "servers", true);
        assertEquals(http.size(), servers.size());
        assertTrue(CollectionUtils.isEqualCollection(http, servers.keySet()));

        http = Arrays.asList(availablePort());
        config.setHttp(http);
        invoke(service, "reloadConfig");
        servers = (Map<Integer, ?>) FieldUtils.readField(service, "servers", true);
        assertEquals(http.size(), servers.size());
        assertTrue(CollectionUtils.isEqualCollection(http, servers.keySet()));
    }


    @Test
    @SuppressWarnings("unchecked")
    public void cleanIdle() throws Exception {
        config.setMaxIdleTime(1);

        http();

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

    @Test(timeout = 60000)
    public void connect_illegal_status() throws Exception {
        out.write("Method host ver others\r\n".getBytes());

        assertCnns(0);
    }

    @Test(timeout = 60000)
    public void http_illegal_target_url() throws Exception {
        out.write("Method host ver\r\n".getBytes());

        assertCnns(0);
    }

    @Test(timeout = 60000)
    public void http_no_connectable_target() throws Exception {
        out.write(("Method http://127.0.0.1:" + availablePort() + " ver\r\n").getBytes());

        Thread.sleep(5000);
        assertCnns(0);
    }

    @Test(timeout = 60000)
    public void http() throws Exception {
        byte[] statusLine = ("Method http://127.0.0.1:" + serverPort + " ver\r\n").getBytes();
        out.write(statusLine);

        serverAccept = server.accept();
        acceptIn = serverAccept.getInputStream();
        assertBs(statusLine, acceptIn);

        byte[] data = rand();
        out.write(data);
        assertBs(data, acceptIn);

        acceptOut = serverAccept.getOutputStream();
        data = rand();
        acceptOut.write(data);
        assertBs(data, in);
    }

    @Test(timeout = 60000)
    public void https_illegal_target() throws Exception {
        out.write("CONNECT host ver\r\n".getBytes());

        assertCnns(0);
    }

    @Test(timeout = 60000)
    public void https_no_connectable_target() throws Exception {
        out.write(("CONNECT 127.0.0.1:" + availablePort() + " ver\r\n").getBytes());

        Thread.sleep(5000);
        assertCnns(0);
    }

    @Test(timeout = 60000)
    public void https_has_return_header() throws Exception {
        byte[] statusLine = ("CONNECT 127.0.0.1:" + serverPort + " ver\r\n").getBytes();
        out.write(statusLine);

        out.write("header1\n".getBytes());
        out.write("header2\r\n".getBytes());
        out.write("\r\n".getBytes());

        assertBs(EstablishedHeaders, in);

        trans();
    }

    @Test(timeout = 60000)
    public void https_no_return_header() throws Exception {
        byte[] statusLine = ("CONNECT 127.0.0.1:" + serverPort + " ver\r\n").getBytes();
        out.write(statusLine);

        out.write("header1\n".getBytes());
        out.write("header2\r\n".getBytes());
        out.write("\n".getBytes());

        assertBs(EstablishedHeaders, in);

        trans();
    }

    private void trans() throws IOException {
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
        Thread.sleep(500);

        Map<Integer, ?> servers = (Map<Integer, ?>) FieldUtils.readField(service, "servers", true);

        Object serverMeta = servers.get(port);
        List<?> connections = (List<?>) FieldUtils.readField(serverMeta, "connections", true);
        assertEquals(expectedSize, connections.size());
    }
}