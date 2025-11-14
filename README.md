### Non-official fork 

The official version is currently maintained at [https://spannm.github.io/ucanaccess/](https://spannm.github.io/ucanaccess/)

This fork is purly for debugging and contrubution purposes.

---

### Welcome to (the new home of) UCanAccess

**UCanAccess** is an open-source pure Java JDBC driver capable of reading and writing Microsoft Access databases.

![Maven Central Version](https://img.shields.io/maven-central/v/io.github.spannm/ucanaccess?label=Maven%20Central)
![Maven Central Last Update](https://img.shields.io/maven-central/last-update/io.github.spannm/ucanaccess?label=Last%20Update)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/spannm/ucanaccess/ci_jdk11_ubuntu.yml?label=Build%20(JDK%2011%20Linux))
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/spannm/ucanaccess/ci_jdk11_win.yml?label=Build%20(JDK%2011%20Win))
![GitHub License](https://img.shields.io/github/license/spannm/ucanaccess)
![GitHub Repo stars](https://img.shields.io/github/stars/spannm/ucanaccess?logoColor=%233a82c2)

---

The project was originally developed by Marco Amadei, Gord Thompson and others and hosted at Sourceforge until version 5.0.1 when development ceased in 2020 and activity on the project sadly died down.

UCanAccess is a very useful piece of software. It would be a shame to see it disappear.
As for myself, I have contributed to UCanAccess in the past and continue to use it to the present day.
I have reached out to my fellow developers but could not reestablish contact.
Therefore, I have forked the latest code base from [Sourceforge](http://ucanaccess.sourceforge.net/site.html) and
make it available at [Github](https://github.com/spannm/ucanaccess).
My fork is intended as a drop-in replacement. It maintains runtime compatibility to prior versions, and is published at [Maven Central](https://central.sonatype.com/artifact/io.github.spannm/ucanaccess) (under groupId io.github.spannm).
The minimum required Java version is now Java 11.
The only compile-time dependencies continue to be Jackcess and HSQLDB/HyperSQL (both have been upgraded to recent CVE-free versions).

I hope to keep on maintaining and releasing UCanAccess, so it can continue to be useful and usable for all of us.

Your feedback, thoughts and contributions are very welcome.

&nbsp;

&nbsp;

---

UCanAccess is licensed under the Apache License, Version 2.0. Please see here for detailed [license info](LICENSE.txt).

Most of the financial functions (PMT, NPER, IPMT, PPMT, RATE, PV) have been originally copied from the Apache Software Foundation's POI project.
They have been then modified and adapted so that they are integrated with UCanAccess in a consistent manner.
The Apache POI project is licensed under Apache License, Version 2.0

Some of the UcanaccessDatabaseMetadata methods have been originally inspired by the hsqldb DatabaseMetaData implementation.
They have been then modified and adapted so that they are integrated with UCanAccess in a consistent manner.
The Hsqldb project is licensed under a BSD-based license.

Microsoft, Access, Microsoft Office, Microsoft Access are trademarks of Microsoft Corporation.
