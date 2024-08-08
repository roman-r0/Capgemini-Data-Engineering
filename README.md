# Capgemini-Data-Engineering

## How to run code (linux):
```shell
pip install pipenv
pipenv install
pipenv shell
python <file>
```

## How to run code (Windows):
Add path for folder to env:
```shell
set PYTHONPATH=%PYTHONPATH%;C:\path\to\your\project\
```
or for powershell
```shell
$env:PYTHONPATH = $pwd
```

```shell
pip install pipenv
pipenv install
pipenv shell
python <path to a file>
```

## How to run tests:
```shell
pip install pipenv
pipenv install
pipenv shell
python -m pytest
```