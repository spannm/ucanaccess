package net.ucanaccess.util;

import static org.assertj.core.api.Assertions.assertThat;

import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class SqlTest extends AbstractBaseTest {

    @Test
    void testEmptyInput() {
        assertThat(Sql.of((CharSequence[]) null).toString()).isBlank();
        assertThat(Sql.of((CharSequence) null).toString()).isBlank();
        assertThat(Sql.of((Collection<CharSequence>) null).toString()).isBlank();
        assertThat(Sql.of(new CharSequence[0]).toString()).isBlank();
        assertThat(Sql.of(new ArrayList<>()).toString()).isBlank();
        assertThat(Sql.of().toString()).isBlank();
    }

    @Test
    void testMultipleStrings() {
        assertThat(Sql.of(List.of("A", "B", "C")))
            .hasToString("A B C");
        assertThat(Sql.of("A", "B", "C"))
            .hasToString("A B C");
    }

    @Test
    void testMultipleTokens() {
        assertThat(Sql.of("SELECT COUNT(*) FROM table",
            "WHERE cond1 = :cond1; "))
                .hasToString("SELECT COUNT(*) FROM table WHERE cond1 = :cond1");
    }

    @ParameterizedTest
    @ValueSource(strings = {" ", "\n", "\t", ";"})
    @NullAndEmptySource
    void testStrip(String str) {
        Sql sql = Sql.of(str);
        assertThat(sql).hasToString("");
    }

    @Test
    void testCharSequenceMethods() {
        Sql sql = Sql.of("SELECT * FROM table");
        assertThat(sql).hasSize(19);
        assertThat(sql.charAt(0)).isEqualTo('S');
        assertThat(sql.subSequence(0, 3)).isEqualTo("SEL");
    }

    @Test
    void testEqualsAndHashCode() {
        Sql sql1 = Sql.of("DROP TABLE table;");
        Sql sql2 = Sql.of(sql1);
        assertThat(sql1).isNotNull()
            .isEqualTo(sql2)
            .hasSameHashCodeAs(sql2)
            .isNotEqualTo(Sql.of());
        assertThat(sql1.toString()).isNotBlank();
    }

}
