package net.ucanaccess.util;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

/**
 * The purpose of this class is to find the project's version information by all means possible,
 * <ul>
 *     <li>while developing the project in an IDE</li>
 *     <li>while running Maven lifecycle goals or</li>
 *     <li>while executing or referencing the project at run-time, whether packaged or not.</li>
 * </ul>.
 * <p>
 * First this class attempts to read version information from key {@code Attributes.Name.IMPLEMENTATION_VERSION}
 * in the Java manifest by using the Java api.
 * <p>
 * Should this operation fail, it looks for a {@code MANIFEST.MF} in the file system in order to
 * read version information from the first file found.
 * <p>
 * In case this also fails, it looks for the project's Maven POM file to extraxct version information from the xml structure.
 *
 * @author Markus Spann
 * @since v5.1.2
 */
public final class VersionInfo {

    /** Cash of version information by class. */
    private static final Map<Class<?>, VersionInfo> CACHE = new ConcurrentHashMap<>();

    private final Logger                            logger;

    private String                                  version;
    private int                                     majorVersion;
    private int                                     minorVersion;

    public static VersionInfo find(Class<?> _class) {
        return CACHE.computeIfAbsent(_class, VersionInfo::new);
    }

    private VersionInfo(Class<?> _class) {
        logger = System.getLogger(getClass().getName());
        version = Objects.requireNonNullElse(_class, getClass()).getPackage().getImplementationVersion();
        if (version != null) {
            logger.log(Level.DEBUG, "Found version ''{0}'' in package", version);
        } else {
            version = readFromManifest();
            if (version == null) {
                version = readFromMavenPom();
            }
        }

        if (version != null) {
            Matcher matcher = Pattern.compile("^([0-9]+)\\.([0-9]+).*").matcher(version);
            if (matcher.matches()) {
                majorVersion = Integer.valueOf(matcher.group(1));
                minorVersion = Integer.valueOf(matcher.group(2));
            } else {
                logger.log(Level.WARNING, "Unparsable implementation version: {0}", version);
            }
        }
    }

    String readFromManifest() {
        String filename = "MANIFEST.MF";
        Path currDir = Paths.get("").toAbsolutePath();
        logger.log(Level.DEBUG, "Searching manifest under current directory ''{0}''", currDir);
        try (Stream<Path> walk = Files.walk(currDir)) {
            Path manifest = walk.filter(p -> p.getFileName().toString().equals(filename)).findFirst().orElse(null);
            if (manifest == null) {
                logger.log(Level.DEBUG, "Could not find ''{0}'' in path ''{1}''", filename, currDir);
            } else {
                try (InputStream is = Files.newInputStream(manifest, StandardOpenOption.READ)) {
                    String ver = new Manifest(is).getMainAttributes().getValue(Attributes.Name.IMPLEMENTATION_VERSION);
                    logger.log(Level.DEBUG, "Found version ''{0}'' in manifest", ver);
                    return ver;
                }
            }
        } catch (IOException _ex) {
            logger.log(Level.WARNING, "Failed to find version info in manifest: {0}", _ex.toString());
        }
        return null;
    }

    String readFromMavenPom() {
        String filename = "pom.xml";
        Path currDir = Paths.get("").toAbsolutePath();
        logger.log(Level.DEBUG, "Searching maven pom under current directory ''{0}''", currDir);
        try (Stream<Path> walk = Files.walk(currDir)) {
            Path pom = walk.filter(p -> p.getFileName().toString().equals(filename)).findFirst().orElse(null);
            if (pom == null) {
                logger.log(Level.WARNING, "Failed to find ''{0}'' in path ''{1}''", filename, currDir);
            } else {
                Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(pom.toFile());

                Element docElem = document.getDocumentElement();
                docElem.normalize();

                NodeList nodeList = docElem.getChildNodes();
                for (int i = 0; i < nodeList.getLength(); i++) {
                    Node node = nodeList.item(i);
                    if (node instanceof Element && "version".equals(node.getNodeName())) {
                        String ver = node.getTextContent();
                        logger.log(Level.DEBUG, "Found version ''{0}'' in maven pom", ver);
                        return ver;
                    }
                }
            }
        } catch (IOException | ParserConfigurationException | SAXException _ex) {
            logger.log(Level.WARNING, "Failed to find version info in maven pom: {0}", _ex.toString());
        }
        return null;
    }

    public String getVersion() {
        return version;
    }

    public int getMajorVersion() {
        return majorVersion;
    }

    public int getMinorVersion() {
        return minorVersion;
    }

    @Override
    public String toString() {
        return String.format("%s[version=%s, major=%d, minor=%d]",
            getClass().getSimpleName(), version, majorVersion, minorVersion);
    }

}
