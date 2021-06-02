package com.pascalming.service;

import com.pascalming.controller.WsMessageService;
import com.pascalming.utils.PascalLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * WsMessageJob
 *
 * @author pascal
 * @date 2021-06-01
 * @description: Ws消息发送Job
 */
@Slf4j
@Component
public class WsMessageJob {
    @Autowired
    WsMessageService wsService;

    /**
     * 每1s发送
     */
    @Scheduled(fixedDelay = 1000)
    public void run(){
        try {
            for ( int i = 0; i < 1000; i ++ ) {
                String msg = PascalLogger.getMsg();
                if ( msg == null)
                    break;
                wsService.broadcastMsg(msg);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
