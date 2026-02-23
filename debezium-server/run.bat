@echo off

SET PATH_SEP=;
SET JAVA_BINARY="C:\Program Files\Java\jdk-17\bin\java"

for %%i in (debezium-server-*runner.jar) do set RUNNER=%%~i

echo %RUNNER%

SET LIB_PATH=lib\*
SET LIB_CONFIG=config\lib
SET DEBEZIUM_OPTIONAL=false

IF "%DEBEZIUM_OPTIONAL%"=="true" SET LIB_PATH=lib\*;lib_opt\*

%JAVA_BINARY% -cp %RUNNER%;conf;%LIB_PATH% io.debezium.server.Main
