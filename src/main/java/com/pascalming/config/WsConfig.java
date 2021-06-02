package com.pascalming.config;

import com.pascalming.controller.WsHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * WsConfig
 *
 * @author pascal
 * @date 2021-06-01
 * @description:Ws 注册
 */

@Configuration
@EnableWebSocket
public class WsConfig implements WebSocketConfigurer {

    @Autowired
    private WsHandler wsHandler;

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry
                .addHandler(wsHandler, "wsSyncLog")
                //允许跨域
                .setAllowedOrigins("*");
    }
}
