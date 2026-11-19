#!/bin/sh

UCANACCESS_HOME=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

if [ -d "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVACMD="$JAVA_HOME/bin/java"
else
    JAVACMD=java
fi

exec "$JAVACMD" -jar "$UCANACCESS_HOME/${artifact.artifactId}-${artifact.version}-${uber.jar.classifier.name}.jar" "$@"
