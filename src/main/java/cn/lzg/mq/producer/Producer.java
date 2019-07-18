package cn.lzg.mq.producer;

import cn.lzg.mq.dto.MessageDto;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsMessagingTemplate;
import org.springframework.stereotype.Component;

import javax.jms.*;

/**
 * 生产者类
 *
 * @Author lzg
 * @Date 2016/12/23 23:19
 */
@Component
public class Producer {

    private static final String USERNAME
            = ActiveMQConnection.DEFAULT_USER;//默认连接用户名
    private static final String PASSWORD
            = ActiveMQConnection.DEFAULT_PASSWORD;//默认连接密码
    private static final String BROKEURL
            = ActiveMQConnection.DEFAULT_BROKER_URL;//默认连接地址
    @Autowired
    private JmsMessagingTemplate jmsMessagingTemplate;
    //
    @Autowired
    private Queue testQueue;
    //
    @Autowired
    private Topic testTopic;
//
//    @Autowired
//    private Queue testQueueObj;

    private final static Logger logger = LoggerFactory.getLogger(Producer.class);

    public void sendQueueText(MessageDto messageDto) {
        logger.info(messageDto.toString());
        this.jmsMessagingTemplate.convertAndSend(this.testQueue, messageDto);
    }

    public void sendTopicText(String msg) {
//        Connection conn = connFactory.createConnection();
//        this.jmsMessagingTemplate.convertAndSend(this.testTopic, msg);
        ConnectionFactory connectionFactory;//连接工厂
        Connection connection = null;//连接

        Session session;//会话 接受或者发送消息的线程

//        TopicPublisher messageProducer;//消息的发送者

        //实例化连接工厂
        connectionFactory = new ActiveMQConnectionFactory(USERNAME, PASSWORD, BROKEURL);
        try {
            //通过连接工厂获取连接
            connection = connectionFactory.createConnection();
//            connection.setClientID("Hello");
            //启动连接
            connection.start();
            //创建session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic destination = session.createTopic("test.topic");

            //创建消息消费者
            MessageProducer messageProducer = session.createProducer(destination);
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            TextMessage textMessage = session.createTextMessage(msg);
            messageProducer.send(textMessage);
        } catch (JMSException e) {
            e.printStackTrace();
        }


    }

//    public void sendQueueObj(MessageDto msg)  {
//        this.jmsMessagingTemplate.convertAndSend(this.testQueueObj, ProtoStuffSerializerUtil.serialize(msg));
//    }
}

