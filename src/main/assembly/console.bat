@ECHO OFF
SETLOCAL

SET "UCANACCESS_HOME=%~dp0"

IF EXIST "%JAVA_HOME%\bin\java.exe" (
    SET "JAVACMD=%JAVA_HOME%\bin\java.exe"
) ELSE (
    SET "JAVACMD=java.exe"
)

"%JAVACMD%" -jar "%UCANACCESS_HOME%${artifact.artifactId}-${artifact.version}-${uber.jar.classifier.name}.jar" %*

REM keep the window open when started by double-click
IF "%~1"=="" PAUSE
