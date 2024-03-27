#!/bin/sh

UCANACCESS_HOME=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
echo $UCANACCESS_HOME

CLASSPATH="$UCANACCESS_HOME/ucanaccess-${project.version}.jar:$UCANACCESS_HOME/lib/hsqldb-${dep.hsqldb.version}.jar:$UCANACCESS_HOME/lib/jackcess-${dep.jackcess.version}.jar"

if [ -d "$JAVA_HOME" -a -x "$JAVA_HOME/bin/java" ]; then
    JAVACMD="$JAVA_HOME/bin/java"
else
    JAVACMD=java
fi

"$JAVACMD" -cp $CLASSPATH ${ucanaccess.console.main}
