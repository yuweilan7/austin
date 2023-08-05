package com.java3y.austin.handler.receiver.kafka;

import cn.hutool.core.util.StrUtil;
import com.java3y.austin.handler.utils.GroupIdMappingUtils;
import com.java3y.austin.support.constans.MessageQueuePipeline;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * 启动消费者
 *
 * @author 3y
 * @date 2021/12/4
 */
@Service
@ConditionalOnProperty(name = "austin.mq.pipeline", havingValue = MessageQueuePipeline.KAFKA)
@Slf4j
public class ReceiverStart {

    @Autowired
    private ApplicationContext context;
    @Autowired
    private ConsumerFactory consumerFactory;

    /**
     * receiver的消费方法常量
     */
    private static final String RECEIVER_METHOD_NAME = "Receiver.consumer";

    /**
     * 获取得到所有的groupId
     */
    private static List<String> groupIds = GroupIdMappingUtils.getAllGroupIds();

    /**
     * 下标(用于迭代groupIds位置)
     */
    private static Integer index = 0;

    /**
     * 为每个渠道不同的消息类型 创建一个Receiver对象
     */
    @PostConstruct
    public void init() {
        for (int i = 0; i < groupIds.size(); i++) {
            context.getBean(Receiver.class);
        }
    }

    /**
     * 给每个Receiver对象的consumer方法 @KafkaListener赋值相应的groupId
     */
    /**
     * 为什么能找到对应的?
     *
     * 这个问题涉及到Spring中的SCOPE_PROTOTYPE和ConcurrentKafkaListenerContainerFactory的工作原理。
     *
     * 首先，这里的@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)注解表示每次从Spring容器中获取Receiver bean时，
     * 都会创建一个新的实例。而在ReceiverStart.init()方法中，对Receiver bean的获取循环了groupIds.size()次，
     * 即每一个groupId对应一个新的Receiver实例。这些Receiver实例在Spring的IoC容器中都是独立的。
     *
     * 然后，对于每一个Receiver实例，其consumer方法上的@KafkaListener注解将它注册为Kafka的一个消息监听器。
     * 这个注解的groupId属性是在ReceiverStart.groupIdEnhancer()方法中设置的，每个Receiver实例都有一个唯一的groupId。
     *
     * 现在我们来看一下ConcurrentKafkaListenerContainerFactory。这个类的实例负责创建Kafka消息监听器的容器。
     * 在ReceiverStart.filterContainerFactory()方法中，我们为这个工厂设置了一个过滤策略，只处理标签匹配的消息。
     * 由于每个Receiver实例都由自己的groupId，因此每个Receiver实例处理的消息是与其groupId匹配的消息。
     *
     * 综上，通过组合使用SCOPE_PROTOTYPE、@KafkaListener以及ConcurrentKafkaListenerContainerFactory，
     * 我们为每一个groupId创建了一个Receiver实例，并设置了只处理与自己groupId匹配的消息。因此，即使消息进来，
     * 也只有对应groupId的Receiver实例会处理。
     * @return
     */
    @Bean
    public static KafkaListenerAnnotationBeanPostProcessor.AnnotationEnhancer groupIdEnhancer() {
        return (attrs, element) -> {
            if (element instanceof Method) {
                String name = ((Method) element).getDeclaringClass().getSimpleName() + StrUtil.DOT + ((Method) element).getName();
                if (RECEIVER_METHOD_NAME.equals(name)) {
                    attrs.put("groupId", groupIds.get(index++));
                }
            }
            return attrs;
        };
    }

    /**
     * 针对tag消息过滤
     * producer 将tag写进header里
     *
     * @return true 消息将会被丢弃
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory filterContainerFactory(@Value("${austin.business.tagId.key}") String tagIdKey,
                                                                          @Value("${austin.business.tagId.value}") String tagIdValue) {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        factory.setAckDiscarded(true);

        factory.setRecordFilterStrategy(consumerRecord -> {
            if (Optional.ofNullable(consumerRecord.value()).isPresent()) {
                for (Header header : consumerRecord.headers()) {
                    if (header.key().equals(tagIdKey) && new String(header.value()).equals(new String(tagIdValue.getBytes(StandardCharsets.UTF_8)))) {
                        return false;
                    }
                }
            }
            return true;
        });
        return factory;
    }
}
