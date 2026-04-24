package company.vk.edu.distrib.compute.martinez1337.controller;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

@FunctionalInterface
public interface RequestProcessor {
    void process(HttpExchange exchange, String id, Integer ack) throws IOException;
}
