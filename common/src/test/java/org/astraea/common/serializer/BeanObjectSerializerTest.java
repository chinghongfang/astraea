package org.astraea.common.serializer;

import org.astraea.common.metrics.BeanObject;
import org.astraea.common.producer.Serializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class BeanObjectSerializerTest {
    @Test
    void testSerialize(){
        var bean = new BeanObject("d", Map.of(), Map.of());
        var serializer = new Serializer.BeanSerializer();

        // 0x00, 0x01 => domain length is 1
        // 0x64       => "d"
        // 0x00       => no properties
        // 0x00       => no attributes
        var bytes = new byte[]{0x00, 0x01, 0x64, 0x00, 0x00};
        Assertions.assertArrayEquals(bytes, serializer.serialize("ignore", List.of(), bean));

        bean = new BeanObject("d", Map.of("a", "bc"), Map.of());
        // 0x00, 0x01 => domain length is 1
        // 0x64       => "d"
        // 0x01       => 1 properties
        // 0x00, 0x01 => key length is 1
        // 0x61       => "a"
        // 0x00, 0x02 => value length is 2
        // 0x62, 0x63 => "bc"
        // 0x00       => no attributes
        bytes = new byte[]{0x00, 0x01, 0x64, 0x01, 0x00, 0x01, 0x61, 0x00, 0x02, 0x62, 0x63, 0x00};
        Assertions.assertArrayEquals(bytes, serializer.serialize("ignore", List.of(), bean));


        bean = new BeanObject("d", Map.of("a", "b"), Map.of("a", "bc"));
        // 0x00, 0x01 => domain length is 1
        // 0x64       => "d"
        // 0x01       => 1 properties
        // 0x00, 0x01 => key length is 1
        // 0x61       => "a"
        // 0x00, 0x01 => value length is 1
        // 0x62       => "b"
        // 0x01       => 1 attributes
        // 0x00, 0x01 => key length is 1
        // 0x61       => "a"
        // 0x09       => type String
        // 0x00, 0x02 => value length is 2
        // 0x62, 0x63 => "bc"
        bytes = new byte[]{0x00, 0x01, 0x64, 0x01, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62, 0x01, 0x00, 0x01, 0x61, 0x09, 0x00, 0x02, 0x62, 0x63};
        Assertions.assertArrayEquals(bytes, serializer.serialize("ignore", List.of(), bean));

    }
}
