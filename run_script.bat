@echo off
echo Activating virtual environment...
call venv\Scripts\activate.bat
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to activate virtual environment. Make sure 'venv\Scripts\activate.bat' exists.
    pause
    exit /b %ERRORLEVEL%
)
echo Virtual environment activated.
echo Running main.py...
cd src
IF %ERRORLEVEL% NEQ 0 (
    echo Failed to change directory to src. Make sure the src folder exists.
    pause
    exit /b %ERRORLEVEL%
)
python main.py
IF %ERRORLEVEL% NEQ 0 (
    echo main.py finished with errors.
    pause
    exit /b %ERRORLEVEL%
)
echo Script finished.
pause
