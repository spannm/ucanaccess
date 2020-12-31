@echo off
set PATH=%PATH%;.
set BASE_DIR=%~f0

:CONT
SET RMVD=%BASE_DIR:~-1%
SET BASE_DIR=%BASE_DIR:~0,-1%
if NOT "%RMVD%"=="\" goto CONT

SET UCANACCESS_HOME=%BASE_DIR%
SET LOCAL_HOME_JAVA="%JAVA_HOME%"
if exist %LOCAL_HOME_JAVA%\bin\java.exe (
  SET LOCAL_JAVA=%LOCAL_HOME_JAVA%\bin\java.exe
) else (
  SET LOCAL_JAVA=java.exe
)

%LOCAL_JAVA% -version
@echo.

SET CLASSPATH="%UCANACCESS_HOME%\lib\hsqldb-2.5.0.jar;%UCANACCESS_HOME%\lib\jackcess-3.0.1.jar;%UCANACCESS_HOME%\lib\commons-lang3-3.8.1.jar;%UCANACCESS_HOME%\lib\commons-logging-1.2.jar;%UCANACCESS_HOME%\ucanaccess-5.0.1.jar"

%LOCAL_JAVA% -classpath %CLASSPATH% net.ucanaccess.console.Main
pause
