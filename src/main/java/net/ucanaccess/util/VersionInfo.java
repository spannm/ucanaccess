package net.ucanaccess.util;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * The purpose of this class is to find the project's version information by all means possible,
 * <ul>
 *     <li>while developing the project in an IDE</li>
 *     <li>while running Maven lifecycle goals or</li>
 *     <li>while executing or referencing the project at run-time, whether packaged or not.</li>
 * </ul>
 * <p>
 * First this class attempts to read version information from key {@code Attributes.Name.IMPLEMENTATION_VERSION}
 * in the Java manifest by using the Java api.
 * <p>
 * Should this operation fail, it looks for a {@code MANIFEST.MF} in the file system in order to
 * read version information from the first file found.
 * <p>
 * In case this also fails, it looks for the project's Maven POM file to extract version information from the xml structure.
 *
 * @author Markus Spann
 * @since v5.1.2
 */
public final class VersionInfo {

    private static final Logger LOGGER = System.getLogger(VersionInfo.class.getName());

    /**
     * Cache of version information by class.
     */
    private static final Map<Class<?>, VersionInfo> CACHE = new ConcurrentHashMap<>();

    private String version;
    private int    majorVersion;
    private int    minorVersion;

    /**
     * Finds and returns the version information for a given class.
     * The method uses a cache to avoid redundant lookups.
     *
     * @param _class The class to find the version for. Cannot be null.
     * @return The VersionInfo instance for the given class.
     * @throws NullPointerException if {@code _class} is null.
     */
    public static VersionInfo find(Class<?> _class) {
        Objects.requireNonNull(_class, "Class required");
        return CACHE.computeIfAbsent(_class, c -> new VersionInfo().findVersion(c.getPackage()));
    }

    /**
     * Clears the internal cache of version information.
     * This is primarily for testing purposes.
     */
    static void clearCache() {
        CACHE.clear();
    }

    /**
     * Default constructor.
     */
    VersionInfo() {
    }

    /**
     * Finds the version information by checking the package, manifest, or Maven POM file.
     *
     * @param _p The package of the class to find the version for.
     * @return This VersionInfo instance with the version details populated.
     */
    VersionInfo findVersion(Package _p) {
        if (_p != null) {
            version = _p.getImplementationVersion();
        }
        if (version != null) {
            LOGGER.log(Level.DEBUG, "Found version ''{0}'' in package", version);
        } else {
            version = readFromManifest();
            if (version == null) {
                version = readFromMavenPom();
            }
        }

        if (version != null) {
            Matcher matcher = Pattern.compile("^(\\d++)\\.?(\\d++)?.*").matcher(version);
            if (matcher.matches()) {
                try {
                    majorVersion = Integer.parseInt(matcher.group(1));
                    if (matcher.groupCount() > 1) {
                        minorVersion = Integer.parseInt(matcher.group(2));
                    }
                } catch (NumberFormatException _ex) {
                    LOGGER.log(Level.WARNING, "Failed to parse major/minor version from: {0}", version);
                }
            } else {
                LOGGER.log(Level.WARNING, "Unparsable implementation version: {0}", version);
            }
        }

        return this;
    }

    /**
     * Reads the version information from a MANIFEST.MF file in the file system.
     *
     * @return The version string, or null if not found.
     */
    String readFromManifest() {
        String filename = "MANIFEST.MF";
        Path currDir = Paths.get("").toAbsolutePath();
        LOGGER.log(Level.DEBUG, "Searching manifest under current directory ''{0}''", currDir);
        try (Stream<Path> walk = Files.walk(currDir)) {
            Path manifest = walk.filter(p -> p.getFileName().toString().equals(filename)).findFirst().orElse(null);
            if (manifest == null) {
                LOGGER.log(Level.DEBUG, "Could not find ''{0}'' in path ''{1}''", filename, currDir);
            } else {
                try (InputStream is = Files.newInputStream(manifest)) {
                    String ver = new Manifest(is).getMainAttributes().getValue(Attributes.Name.IMPLEMENTATION_VERSION);
                    LOGGER.log(Level.DEBUG, "Found version ''{0}'' in manifest", ver);
                    return ver;
                }
            }
        } catch (IOException _ex) {
            LOGGER.log(Level.WARNING, "Failed to find version info in manifest: {0}", _ex.getMessage());
        }
        return null;
    }

    /**
     * Reads the version information from a pom.xml file in the file system.
     *
     * @return The version string, or null if not found.
     */
    String readFromMavenPom() {
        String filename = "pom.xml";
        Path currDir = Paths.get("").toAbsolutePath();
        LOGGER.log(Level.DEBUG, "Searching maven pom under current directory ''{0}''", currDir);
        try (Stream<Path> walk = Files.walk(currDir)) {
            Path pom = walk.filter(p -> p.getFileName().toString().equals(filename)).findFirst().orElse(null);
            if (pom == null) {
                LOGGER.log(Level.WARNING, "Failed to find ''{0}'' in path ''{1}''", filename, currDir);
            } else {
                Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(pom.toFile());
                document.getDocumentElement().normalize();

                String ver = (String) XPathFactory.newInstance().newXPath()
                    .evaluate("/project/version", document, XPathConstants.STRING);

                if (ver != null && !ver.isEmpty()) {
                    LOGGER.log(Level.DEBUG, "Found version ''{0}'' in maven pom", ver);
                    return ver;
                }
            }
        } catch (IOException | ParserConfigurationException | SAXException | XPathExpressionException _ex) {
            LOGGER.log(Level.WARNING, "Failed to find version info in maven pom: {0}", _ex.getMessage());
        }
        return null;
    }

    /**
     * Returns the full version string.
     *
     * @return The version string, or null if not found.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Returns the major version number.
     *
     * @return The major version number, or 0 if not found or unparsable.
     */
    public int getMajorVersion() {
        return majorVersion;
    }

    /**
     * Returns the minor version number.
     *
     * @return The minor version number, or 0 if not found or unparsable.
     */
    public int getMinorVersion() {
        return minorVersion;
    }

    /**
     * Returns a string representation of the version info.
     *
     * @return A formatted string with version, major, and minor numbers.
     */
    @Override
    public String toString() {
        return String.format("%s[version=%s, major=%d, minor=%d]",
            getClass().getSimpleName(), version, majorVersion, minorVersion);
    }

}
