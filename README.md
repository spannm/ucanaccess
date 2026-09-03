<div align="center">
  <a href="https://central.sonatype.com/artifact/io.github.spannm/ucanaccess"><img src="https://img.shields.io/maven-central/v/io.github.spannm/ucanaccess?label=Maven%20Central&style=flat-square" alt="Maven Central Version"></a>
  <img src="https://img.shields.io/maven-central/last-update/io.github.spannm/ucanaccess?label=Updated&style=flat-square&color=blue" alt="Maven Central Last Update">
  <a href="https://github.com/spannm/ucanaccess/stargazers"><img src="https://img.shields.io/github/stars/spannm/ucanaccess?logo=github&label=&logoColor=white&labelColor=555555&color=007ec6&style=flat-square" alt="GitHub Stars"></a>
  <br>
  <a href="https://github.com/spannm/ucanaccess/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/spannm/ucanaccess/ci.yml?label=Build&style=flat-square" alt="GitHub Actions Workflow Status"></a>
  <a href="https://github.com/spannm/ucanaccess/actions/workflows/codeql.yml"><img src="https://img.shields.io/github/actions/workflow/status/spannm/ucanaccess/codeql.yml?label=CodeQL&style=flat-square" alt="CodeQL Workflow Status"></a>
  <a href="https://javadoc.io/doc/io.github.spannm/ucanaccess"><img src="https://javadoc.io/badge2/io.github.spannm/ucanaccess/javadoc.svg?style=flat-square" alt="Javadoc"></a>
  <a href="https://apidia.net/mvn/io.github.spannm/ucanaccess"><img src="https://apidia.net/mvn/io.github.spannm/ucanaccess/badge_flat_square.svg" alt="APIdia"></a>
</div>

<h1 align="center">UCanAccess</h1>
<h3 align="center">The modern bridge between Java and Microsoft Access</h3>

**UCanAccess** is a open-source pure-Java JDBC driver that gives you seamless access to Microsoft Access databases (`.mdb`, `.accdb`) without needing any native Windows libraries - R.I.P. `OdbcJdbcBridge` 🪦.

<p>
  <strong>New to UCanAccess?</strong> Check out the introduction guide on 
  <a href="https://foojay.io/today/ucanaccess-java-ms-access-jdbc-guide/">Foojay.io: UCanAccess – Java MS Access JDBC Guide</a>
</p>

<div align="center">
  <img src="docs/images/ucanaccess-logo.svg" alt="UCanAccess Logo" width="360">
</div>

## ✨ Key Features

* **Pure Java Power**: Zero native dependencies. Runs anywhere Java 11+ is supported.

* **Drop-in Replacement**: Fully compatible with previous UCanAccess versions.

* **Modern Core**: Built on top of the latest **Jackcess 5.1.6** and **HSQLDB 2.7.4** for maximum stability and security.

* **Comprehensive SQL Support**: Supports SELECT, INSERT, UPDATE, DELETE, and even complex DDL operations like `ALTER TABLE`.

* **Access-like Logic**: Includes built-in Access functions (like `IIf`, `Nz`, and financial functions like `PMT` or `PV`).

* **Security Conscious**: Regularly updated to be free of known CVEs.

<p style="height: 20px;">&nbsp;</p>

## 🛠 Tech Stack & Requirements

* **Java Version**: 11 or higher (LTS versions like Java 17 and 21 are fully supported and tested).

* **Build Tool**: [Maven](https://maven.apache.org/)

* **Main Dependencies**:

    * [Jackcess](https://github.com/spannm/jackcess/)

    * [HyperSQL Database (HSQLDB)](http://hsqldb.org/)

<p style="height: 20px;">&nbsp;</p>

## 📦 Installation

To use UCanAccess in your project, add the following dependency.

### Maven (`pom.xml`)

```xml
<dependency>
    <groupId>io.github.spannm</groupId>
    <artifactId>ucanaccess</artifactId>
    <version>5.1.7</version>
</dependency>
```

### Gradle (Groovy / `build.gradle`)

```groovy
implementation 'io.github.spannm:ucanaccess:5.1.7'
```

### Gradle (Kotlin DSL / `build.gradle.kts`)

```kotlin
implementation("io.github.spannm:ucanaccess:5.1.7")
```

<p style="height: 20px;">&nbsp;</p>

## 🔁 Migrating from `net.sf.ucanaccess`

Still on the original `net.sf.ucanaccess:ucanaccess` artifact? That project has had no release since 2020 and no longer receives security or compatibility updates. Switching to this fork is a **drop-in, API-compatible change** — no code changes required, just update your dependency coordinates:

```diff
- <groupId>net.sf.ucanaccess</groupId>
+ <groupId>io.github.spannm</groupId>
  <artifactId>ucanaccess</artifactId>
- <version>5.0.1</version>
+ <version>5.1.7</version>
```

You get current Jackcess/HSQLDB versions (with their CVE fixes), tested Java 11/17/21 support on Linux and Windows, and active maintenance — with the same JDBC driver class and connection URL you already use.

<p style="height: 20px;">&nbsp;</p>

## 🚦 Quick Start

Connecting to your database is as simple as:

```java
import java.sql.Connection;
import java.sql.DriverManager;

String url = "jdbc:ucanaccess://C:/path/to/your/database.accdb";
try (Connection conn = DriverManager.getConnection(url)) {
    // your code here
}
```

<p style="height: 20px;">&nbsp;</p>

## 🔗 Linked Tables & Untrusted Databases

If the `.accdb`/`.mdb` file you open contains linked tables, UCanAccess automatically opens the linked database files
so those tables can be read. Local linked paths are resolved as before, but a linked table pointing to a
**network/UNC path** (e.g. `\\server\share\linked.accdb`) is rejected by default with an `AccessDeniedException` —
such a path is taken verbatim from the database file, so a database from an untrusted source could otherwise make
your JVM reach out to an arbitrary host as soon as the linked table is touched.

If you trust the linked network paths in a given database, opt back in explicitly via the `allowRemoteLinks`
connection property:

```java
String url = "jdbc:ucanaccess://C:/path/to/your/database.accdb;allowRemoteLinks=true";
```

<p style="height: 20px;">&nbsp;</p>

## ❤️ Why this Fork?

The original project (developed by Marco Amadei and Gord Thompson) was the gold standard for Access connectivity but went quiet in 2020, leaving `net.sf.ucanaccess:ucanaccess` unmaintained at version 5.0.1.
As a long-time contributor and Java enthusiast, I decided to give UCanAccess a **new home**.

My goal is to keep this essential tool alive, maintain a **high test coverage** (JUnit 5), and ensure it meets modern **Clean Code** and **SOLID** standards. If you maintain a project that still depends on `net.sf.ucanaccess`, see [Migrating from `net.sf.ucanaccess`](#-migrating-from-netsfucanaccess) above — a PR bumping the dependency is very welcome.

<p style="height: 20px;">&nbsp;</p>

## 🤝 Contributions welcome!

Got a bug to fix or a feature to add?

1. Check out the [Issues](https://github.com/spannm/ucanaccess/issues)
2. [Fork](https://github.com/spannm/ucanaccess/fork) the Repo
3. Submit a [Pull Request](https://github.com/spannm/ucanaccess/pulls)

*Note: Please ensure your code follows the project's quality standards (Checkstyle, PMD are enforced in the build)*.

<div align="center"> ─────────────── </div>

### ⚖️ License

UCanAccess is licensed under the **Apache License, Version 2.0**.

<p style="height: 40px;">&nbsp;</p>

<div align="center">
<table style="border-collapse: collapse;">
  <tr>
    <td style="padding: 40px; border: 2px solid #3a82c2;">
      <strong>Enjoying UCanAccess? Please leave a 🌟 to support the project!</strong><br>
      <small>Your stars help to keep the bridge between Java and Access alive and visible.</small>
    </td>
  </tr>
</table>
</div>
