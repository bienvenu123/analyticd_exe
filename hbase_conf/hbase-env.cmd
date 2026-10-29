@echo off
if not defined JAVA_HOME (
    echo Set JAVA_HOME to a supported JDK installation before starting HBase.
    exit /b 1
)
if not defined HBASE_LOG_DIR set "HBASE_LOG_DIR=%TEMP%\auca_hbase_logs"
set HBASE_HEAPSIZE=1024
