@echo off
set "PROJECT_FOLDER=C:\Users\FashionForth\PycharmProjects\scrape_voila"
set "VIRTUAL_ENV_RUN=%PROJECT_FOLDER%\venv\bin\activate"
set "PYTHON=%PROJECT_FOLDER%\venv\bin\python.exe"

"%VIRTUAL_ENV_RUN%" & "%PYTHON%" "%PROJECT_FOLDER%\main.py"
pause