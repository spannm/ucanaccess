#!/bin/sh

UCANACCESS_HOME=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

if [ -d "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVACMD="$JAVA_HOME/bin/java"
else
    JAVACMD=java
fi

# the lib folder holds the optional jars for encrypted databases
CLASSPATH="$UCANACCESS_HOME/${artifact.artifactId}-${artifact.version}-${uber.jar.classifier.name}.jar:$UCANACCESS_HOME/lib/*"

# rlwrap, if installed, adds line editing and a history kept in ~/.ucanaccess_history,
# it needs a terminal that reports its width (not the case in emacs, for example)
WIDTH=$(stty size 2> /dev/null | cut -d ' ' -f 2)
if [ -t 0 ] && [ "${WIDTH:-0}" -gt 0 ] && command -v rlwrap > /dev/null 2>&1; then
    exec rlwrap -C ucanaccess "$JAVACMD" -cp "$CLASSPATH" ${ucanaccess.console.main} "$@"
fi

exec "$JAVACMD" -cp "$CLASSPATH" ${ucanaccess.console.main} "$@"
