package net.ucanaccess.test;

import net.ucanaccess.test.AccessVersionSource.AccessVersionArgumentsProvider;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@code @AccessVersionSource} is an {@link ArgumentsSource} that provides Microsoft Access version enum constants to to test cases.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ArgumentsSource(AccessVersionArgumentsProvider.class)
public @interface AccessVersionSource {

    /**
     * Optional names of enum constants to include.<br>
     * If specified, the names must match existing enum constants otherwise an {@link IllegalArgumentException} is thrown.<br>
     * If not specified, all enum constants are taken into consideration.
     */
    String[] include() default {};

    /**
     * Optional names of enum constants to exclude.<br>
     * If used, the names must match existing enum constants otherwise an {@link IllegalArgumentException} is thrown.
     */
    String[] exclude() default {};

    class AccessVersionArgumentsProvider implements ArgumentsProvider {

        @Override
        public Stream<Arguments> provideArguments(ExtensionContext _context) throws Exception {
            AccessVersionSource src = _context.getElement().map(elem -> AnnotationSupport.findAnnotation(elem, AccessVersionSource.class).get()).orElse(null);

            List<AccessVersion> include = Arrays.stream(src.include()).map(AccessVersion::valueOf).collect(Collectors.toList());
            if (include.isEmpty()) {
                include.addAll(Arrays.asList(AccessVersion.values()));
            }
            List<AccessVersion> exclude = Arrays.stream(src.exclude()).map(AccessVersion::valueOf).collect(Collectors.toList());

            return include.stream()
                .filter(ff -> !exclude.contains(ff))
                .map(Arguments::of);
        }

    }

}
