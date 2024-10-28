package net.ucanaccess.jdbc;

import static net.ucanaccess.converters.Metadata.Property.*;
import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.converters.Metadata.Property;
import net.ucanaccess.test.UcanaccessBaseTest;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

class UcanaccessDriverTest extends UcanaccessBaseTest {

    @Test
    void testNormalizeProperties() {
        Properties input = new Properties();
        input.setProperty("columnOrder", "data");
        input.setProperty("ConcatNulls", "false"); // overwritten by url
        String url = "jdbc:ucanaccess:///tmp/testdb.mdb;CONCATNULLS=true;bo=gus;withoutVal1;withoutVal2=;enCrypt=false";
        Map<String, String> unknownProps = new LinkedHashMap<>();

        Map<Property, String> output = UcanaccessDriver.readProperties(input, url, unknownProps::put);

        assertThat(output).hasSize(3)
            .containsEntry(columnOrder, "data")
            .containsEntry(concatNulls, "true")
            .containsEntry(encrypt, "false");

        assertThat(unknownProps).hasSize(3)
            .containsEntry("bo", "gus")
            .containsEntry("withoutVal1", null)
            .containsEntry("withoutVal2", null);
    }

    @Test
    void testVersion() {
        UcanaccessDriver driver = new UcanaccessDriver();

        assertThat(driver.getMajorVersion()).isGreaterThanOrEqualTo(5);
        assertThat(driver.getMinorVersion()).isGreaterThanOrEqualTo(1);
    }

}
