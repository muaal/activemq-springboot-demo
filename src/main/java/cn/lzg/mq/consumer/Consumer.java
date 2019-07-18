package cn.lzg.mq.consumer;

import cn.lzg.mq.dto.MessageDto;
import cn.lzg.mq.utils.ProtoStuffSerializerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

/**
 * 消费者类
 *
 * @Author lzg
 * @Date 2016/12/23 23:29
 */
@Component
public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(getClass());


//    @JmsListener(destination = "test.queue", containerFactory = "jmsListenerContainerQueue")
//    private void getQueueMessageText(final String text) {
//        /*try {
//            // 模拟业务处理
//            System.out.println("getMessageText 休息10s");
//            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
//            System.out.println("继续运行");
//        } catch (InterruptedException e) {
//            throw new RuntimeException("业务处理异常");
//        }*/
//        logger.info("消费者消费一条信息{}。",text);
//
//    }

//    @JmsListener(destination = "test.topic", containerFactory = "jmsListenerContainerTopic")
//    private void getTopicMessageText(final String text) {
//        /*try {
//            // 模拟业务处理
//            System.out.println("getTopicMessageText 休息10s");
//            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
//            System.out.println("继续运行");
//        } catch (InterruptedException e) {
//            throw new RuntimeException("业务处理异常");
//        }*/
//        System.out.println("*****************************************************************");
//        System.out.println("线程ID = " + Thread.currentThread().getId());
//        System.out.println("消费者接受topic信息：" + text);
//    }

    @JmsListener(destination = "test.queue.obj", containerFactory = "jmsListenerContainerQueue")
    private void getQueueMessageObj(final byte[] msg) {
        /*try {
            // 模拟业务处理
            System.out.println("getMessageText 休息10s");
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            System.out.println("继续运行");
        } catch (InterruptedException e) {
            throw new RuntimeException("业务处理异常");
        }*/
        System.out.println("##################################################################");
        System.out.println("线程ID = " + Thread.currentThread().getId());
        System.out.println("消费者接受queueObj信息：" + ProtoStuffSerializerUtil.deserialize(msg, MessageDto.class));

    }
}
