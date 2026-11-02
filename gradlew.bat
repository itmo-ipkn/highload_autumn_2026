@echo off
setlocal EnableExtensions EnableDelayedExpansion
set GRADLE_VERSION=9.8.2
if "%GRADLE_USER_HOME%"=="" set GRADLE_USER_HOME=%USERPROFILE%\.gradle
set DIST_DIR=%GRADLE_USER_HOME%\wrapper\dists\gradle-%GRADLE_VERSION%-bin\manual
set GRADLE_BIN=%DIST_DIR%\gradle-%GRADLE_VERSION%\bin\gradle.bat
if not exist "%GRADLE_BIN%" (
  if not exist "%DIST_DIR%" mkdir "%DIST_DIR%"
  set ZIP=%DIST_DIR%\gradle-%GRADLE_VERSION%-bin.zip
  if not exist "!ZIP!" powershell -NoProfile -Command "Invoke-WebRequest https://services.gradle.org/distributions/gradle-%GRADLE_VERSION%-bin.zip -OutFile '!ZIP!'"
  powershell -NoProfile -Command "$h=(Get-FileHash '!ZIP!' -Algorithm SHA256).Hash.ToLower(); if($h -ne '21bd5da2de49b642904077f38c0efdc7778e92b3ed0889392dca61b97e389607'){exit 1}"
  if errorlevel 1 exit /b 1
  powershell -NoProfile -Command "Expand-Archive -Force '!ZIP!' '%DIST_DIR%'"
  if errorlevel 1 exit /b 1
)
call "%GRADLE_BIN%" %*
endlocal
