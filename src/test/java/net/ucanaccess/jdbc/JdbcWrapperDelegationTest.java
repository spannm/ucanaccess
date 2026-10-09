package net.ucanaccess.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

import net.ucanaccess.exception.UcanaccessSQLException;
import net.ucanaccess.test.AbstractBaseTest;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.Invocation;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Verifies that the JDBC wrapper classes pass calls on to the wrapped HSQLDB object
 * and translate a {@link SQLException} into a {@link UcanaccessSQLException}.
 */
class JdbcWrapperDelegationTest extends AbstractBaseTest {

    @Test
    void testResultSetDelegates() throws SQLException {
        assertDelegation(ResultSet.class, d -> new UcanaccessResultSet(d, null), Set.of(
            // row changes are written through to the Access database via the statement's connection
            "deleteRow", "insertRow", "updateRow",
            // wraps the metadata to resolve column aliases
            "getMetaData",
            // passes floats on as BigDecimal to avoid binary rounding
            "updateFloat",
            // parses Access hyperlink values
            "getURL"));
    }

    @Test
    void testDatabaseMetaDataDelegates() throws SQLException {
        assertDelegation(DatabaseMetaData.class, d -> new UcanaccessDatabaseMetadata(d, mock(UcanaccessConnection.class)), Set.of(
            // queries joining HSQLDB's INFORMATION_SCHEMA with UCanAccess metadata
            "getBestRowIdentifier", "getClientInfoProperties", "getColumnPrivileges", "getColumns", "getCrossReference",
            "getExportedKeys", "getImportedKeys", "getIndexInfo", "getPrimaryKeys", "getTablePrivileges", "getTables",
            // not supported
            "getCatalogs", "getRowIdLifetime", "getSchemas",
            // describe UCanAccess and the Access database rather than HSQLDB
            "getConnection", "getDatabaseProductName", "getDatabaseProductVersion", "getDriverMajorVersion",
            "getDriverMinorVersion", "getDriverName", "getDriverVersion", "getIdentifierQuoteString", "getSchemaTerm",
            "getURL", "generatedKeyAlwaysReturned"));
    }

    /**
     * Calls every interface method on a wrapper around a mock and checks that the mock received a call of the same
     * name. Then repeats with a mock that throws, expecting a {@code UcanaccessSQLException}.
     *
     * @param iface the JDBC interface
     * @param wrapperFactory creates the wrapper around a delegate
     * @param ownLogic names of methods that are implemented by the wrapper itself instead of delegating
     */
    private static <I> void assertDelegation(Class<I> iface, Function<I, I> wrapperFactory, Set<String> ownLogic) throws SQLException {
        List<Method> methods = Arrays.stream(iface.getMethods())
            .filter(m -> !m.isDefault() && !Modifier.isStatic(m.getModifiers()))
            .filter(m -> !ownLogic.contains(m.getName()))
            .sorted(Comparator.comparing(Method::toString))
            .collect(Collectors.toList());

        List<String> failures = new ArrayList<>();

        I delegate = mock(iface);
        stubMetaData(delegate);
        I wrapper = wrapperFactory.apply(delegate);
        for (Method m : methods) {
            clearInvocations(delegate);
            try {
                m.invoke(wrapper, dummyArgs(m));
            } catch (InvocationTargetException ex) {
                failures.add("delegate " + m.getName() + ": " + ex.getCause());
                continue;
            } catch (ReflectiveOperationException ex) {
                failures.add("delegate " + m.getName() + ": " + ex);
                continue;
            }
            boolean delegated = mockingDetails(delegate).getInvocations().stream()
                .map(Invocation::getMethod)
                .anyMatch(dm -> dm.getName().equals(m.getName()));
            if (!delegated) {
                failures.add("delegate " + m.getName() + ": not delegated");
            }
        }

        I throwing = mock(iface, inv -> {
            throw new SQLException("mock");
        });
        I throwingWrapper = wrapperFactory.apply(throwing);
        for (Method m : methods) {
            if (!Arrays.asList(m.getExceptionTypes()).contains(SQLException.class)) {
                continue;
            }
            try {
                m.invoke(throwingWrapper, dummyArgs(m));
                failures.add("translate " + m.getName() + ": no exception");
            } catch (InvocationTargetException ex) {
                if (!(ex.getCause() instanceof UcanaccessSQLException)) {
                    failures.add("translate " + m.getName() + ": " + ex.getCause());
                }
            } catch (ReflectiveOperationException ex) {
                failures.add("translate " + m.getName() + ": " + ex);
            }
        }

        assertThat(failures).as("%s methods", iface.getSimpleName()).isEmpty();
    }

    private static void stubMetaData(Object delegate) throws SQLException {
        if (delegate instanceof ResultSet) {
            when(((ResultSet) delegate).getMetaData()).thenReturn(mock(ResultSetMetaData.class));
        }
    }

    private static Object[] dummyArgs(Method m) {
        return Arrays.stream(m.getParameterTypes()).map(JdbcWrapperDelegationTest::dummyValue).toArray();
    }

    private static Object dummyValue(Class<?> type) {
        if (type == boolean.class) {
            return false;
        } else if (type == int.class) {
            return 1;
        } else if (type == long.class) {
            return 1L;
        } else if (type == short.class) {
            return (short) 1;
        } else if (type == byte.class) {
            return (byte) 1;
        } else if (type == float.class) {
            return 1f;
        } else if (type == double.class) {
            return 1d;
        } else if (type == String.class) {
            return "COL";
        } else if (type.isArray()) {
            return Array.newInstance(type.getComponentType(), 0);
        }
        return null;
    }

}
