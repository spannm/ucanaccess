# UcanAccess :: Example :: Password Protection

This example demonstrates how to connect to a password-protected MS Access (`.accdb`) file using **UcanAccess** and the **Jackcess Encrypt** extension.

## Features

* **Custom Jackcess Opener**: Implementation of `IJackcessOpenerInterface` to handle the encryption handshake.
* **Automated Build**: Configured Maven `defaultGoal` for a one-command "build and run"

## Prerequisites

* **Java**: JDK 11 or higher
* **Maven**: 3.9+ recommended

## Quick Start

The project is pre-configured to run the main demo immediately.

Simply execute the following command in the root directory: `mvn`

This will trigger `clean compile exec:java`, which:
1.  Compiles the source code
2.  Connects to the bundled `password-protected.accdb` (Password: `alligator3`)
3.  Executes sample queries to verify decrypted access

## Components

### PasswordProtectedOpener
UcanAccess requires a bridge to the underlying encryption engine. This class uses `CryptCodecProvider` from the `jackcess-encrypt` library to provide the password to the database builder.

### PasswordProtectedDemo
The main entry point. It demonstrates the JDBC connection string format:
`jdbc:ucanaccess://<path>;jackcessOpener=net.ucanaccess.example.PasswordProtectedOpener`

<p style="height: 40px;">&nbsp;</p>

<div align="center">
<table style="border-collapse: collapse;">
  <tr>
    <td style="padding: 40px; border: 2px solid #3a82c2;">
      <strong>Enjoying UCanAccess and Jackcess? Please leave a 🌟 to support the projects!</strong><br>
      <small>Your stars help to keep this open source development visible and going.</small>
    </td>
  </tr>
</table>
</div>


