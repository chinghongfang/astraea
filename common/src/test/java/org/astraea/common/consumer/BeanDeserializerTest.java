package org.astraea.common.consumer;

import org.astraea.common.metrics.BeanObject;
import org.astraea.common.producer.Serializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class BeanDeserializerTest {
    @Test
    void testDeserialize(){
        var bean = new BeanObject("domainName", Map.of("name", "Size"), Map.of("Mean", 3.9, "Count", 70L));
        var serializer = new Serializer.BeanSerializer();
        var deserializer = new Deserializer.BeanDeserializer();

        var newBean = deserializer.deserialize("ignore", List.of(), serializer.serialize("ignore", List.of(), bean));
        Assertions.assertEquals(bean.domainName(), newBean.domainName());
        Assertions.assertEquals(bean.properties(), newBean.properties());
        Assertions.assertEquals(bean.attributes(), newBean.attributes());
    }
}
