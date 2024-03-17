package net.ucanaccess.test;

import net.ucanaccess.test.AccessDefaultVersionSource.DefaultAccessVersionArgumentsProvider;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.stream.Stream;

/**
 * {@code @DefaultAccessVersionSource} is an {@link ArgumentsSource} that provides the default Microsoft Access version to test cases.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ArgumentsSource(DefaultAccessVersionArgumentsProvider.class)
public @interface AccessDefaultVersionSource {

    static class DefaultAccessVersionArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<Arguments> provideArguments(ExtensionContext _context) throws Exception {
            return Stream.of(AccessVersion.getDefaultAccessVersion()).map(Arguments::of);
        }

    }

}
