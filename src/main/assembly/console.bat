@ECHO OFF
SETLOCAL

SET "UCANACCESS_HOME=%~dp0"

IF EXIST "%JAVA_HOME%\bin\java.exe" (
    SET "JAVACMD=%JAVA_HOME%\bin\java.exe"
) ELSE (
    SET "JAVACMD=java.exe"
)

REM the lib folder holds the optional jars for encrypted databases
SET "CLASSPATH=%UCANACCESS_HOME%${artifact.artifactId}-${artifact.version}-${uber.jar.classifier.name}.jar;%UCANACCESS_HOME%lib\*"

"%JAVACMD%" -cp "%CLASSPATH%" ${ucanaccess.console.main} %*

REM keep the window open when started by double-click
IF "%~1"=="" PAUSE
