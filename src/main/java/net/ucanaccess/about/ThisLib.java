package net.ucanaccess.about;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.CodeSource;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Provides metadata and build information about this library when executed directly via CLI.
 */
public final class ThisLib {

    private ThisLib() {
    }

    /**
     * Main entry point when the JAR file is executed directly.
     * <p>
     * Reads the {@code MANIFEST.MF} file from the JAR package and prints formatted metadata
     * to {@code System.out}.
     *
     * @param args command-line arguments (unused)
     */
    public static void main(String[] args) {
        readManifest().ifPresent(m -> System.out.print(buildInfo(m)));
    }

    /**
     * Builds the formatted metadata text printed when the JAR file is executed directly.
     *
     * @param manifest the manifest to read metadata from, may be {@code null}
     * @return the formatted metadata text, terminated with a trailing line separator
     */
    static String buildInfo(Manifest manifest) {
        String title = getAttribute(manifest, "Implementation-Title", "Project-Name").orElse("Java Library");
        String version = getAttribute(manifest, "Implementation-Version", "Project-Version").orElse("unknown");
        String description = getAttribute(manifest, "Project-Description").orElse("");

        String gitCommit = getAttribute(manifest, "Git-Commit-Id", "X-BasePOM-Git-Commit-Id").orElse(null);

        String nl = System.lineSeparator();
        StringBuilder sb = new StringBuilder(nl);
        sb.append(title).append(" v").append(version).append(nl);

        if (!description.isEmpty()) {
            sb.append(description).append(nl);
        }
        sb.append(nl);

        List<Map.Entry<String, String>> metadata = Arrays.asList(
            entry("Vendor",      getAttribute(manifest, "Implementation-Vendor").orElse(null)),
            entry("Build JDK",   getAttribute(manifest, "Build-Jdk-Spec").orElse(null)),
            entry("Build Time",  getAttribute(manifest, "Build-Time").orElse(null)),
            entry("Git Commit",  gitCommit),
            entry("Git Branch",  getAttribute(manifest, "Git-Branch").orElse(null)),
            entry("Homepage",    getAttribute(manifest, "Project-Url").orElse(null)),
            entry("Issues",      getAttribute(manifest, "Issue-Management-Url").orElse(null)),
            entry("Source Code", getAttribute(manifest, "Scm-Url").orElse(null)));

        for (Map.Entry<String, String> e : metadata) {
            if (e.getValue() != null && !e.getValue().isEmpty()) {
                sb.append(String.format("%-15s: %s", e.getKey(), e.getValue())).append(nl);
            }
        }

        sb.append(nl);
        sb.append("Please note: This JAR is a software library and not intended for direct CLI execution.")
            .append(nl);

        return sb.toString();
    }

    /**
     * Creates an immutable map entry representing a key-value metadata pair.
     *
     * @param key metadata key
     * @param value metadata value
     * @return immutable map entry
     */
    private static Map.Entry<String, String> entry(String key, String value) {
        return new SimpleImmutableEntry<>(key, value);
    }

    /**
     * Resolves and parses the {@code MANIFEST.MF} file from the location of this class's code source.
     *
     * @return an {@link Optional} containing the parsed {@link Manifest}, or empty if unavailable
     */
    private static Optional<Manifest> readManifest() {
        CodeSource codeSource = ThisLib.class.getProtectionDomain().getCodeSource();
        if (codeSource == null) {
            return Optional.empty();
        }

        URL location = codeSource.getLocation();
        if (location == null) {
            return Optional.empty();
        }

        try {
            URL manifestUrl = new URL("jar:" + location.toExternalForm() + "!/META-INF/MANIFEST.MF");
            try (InputStream is = manifestUrl.openStream()) {
                return Optional.of(new Manifest(is));
            }
        } catch (IOException ignored) {
            // running outside JAR context (e.g., IDE) is expected
            return Optional.empty();
        }
    }

    /**
     * Searches for the given attribute names in the main attributes first.
     * <p>
     * If not found, searches across all named individual sections in the manifest.
     *
     * @param manifest the manifest to search in, may be {@code null}
     * @param attributeNames one or more attribute names to look for
     * @return an {@link Optional} containing the first non-blank attribute value, or empty
     */
    private static Optional<String> getAttribute(Manifest manifest, String... attributeNames) {
        if (manifest == null || attributeNames == null) {
            return Optional.empty();
        }

        for (String attrName : attributeNames) {
            Optional<String> value = findInAttributes(manifest.getMainAttributes(), attrName);
            if (value.isPresent()) {
                return value;
            }

            for (Attributes sectionAttributes : manifest.getEntries().values()) {
                value = findInAttributes(sectionAttributes, attrName);
                if (value.isPresent()) {
                    return value;
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Extract and trims an attribute value from the given attributes object.
     *
     * @param attributes the attributes container, may be {@code null}
     * @param name the name of the attribute
     * @return an {@link Optional} containing the trimmed value, or empty if null or blank
     */
    private static Optional<String> findInAttributes(Attributes attributes, String name) {
        if (attributes == null || name == null) {
            return Optional.empty();
        }
        String val = attributes.getValue(name);
        if (val == null) {
            return Optional.empty();
        }
        String trimmed = val.trim();
        return trimmed.isEmpty() ? Optional.empty() : Optional.of(trimmed);
    }

}

