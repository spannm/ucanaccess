package net.ucanaccess.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DBReferenceTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "\\\\server\\share\\linked.accdb",
        "//server/share/linked.accdb",
        "\\\\?\\UNC\\server\\share\\linked.accdb",
        "\\\\.\\linked.accdb",
        "\\/server/share/linked.accdb"
    })
    void testIsNetworkPathDetectsUncAndNetworkPaths(String _linkedDbName) {
        assertTrue(DBReference.isNetworkPath(_linkedDbName));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "linked.accdb",
        "../outside.accdb",
        "..\\outside.accdb",
        "c:\\db\\linked.accdb",
        "/home/user/db/linked.accdb"
    })
    void testIsNetworkPathAllowsLocalPaths(String _linkedDbName) {
        assertFalse(DBReference.isNetworkPath(_linkedDbName));
    }

}
