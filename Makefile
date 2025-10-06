VENV := .venv
PYTHON := python3
PIP := $(VENV)/bin/pip
UVICORN := $(VENV)/bin/uvicorn

STORAGE_BACKEND ?= local
LOCAL_STORAGE_PATH ?= $(CURDIR)/data
PORT ?= 8080

.PHONY: venv install run clean

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -e .

run: install
	STORAGE_BACKEND=$(STORAGE_BACKEND) \
	LOCAL_STORAGE_PATH=$(LOCAL_STORAGE_PATH) \
	PORT=$(PORT) \
	PYTHONPATH=src \
	$(UVICORN) main:application --host 0.0.0.0 --port $(PORT)

clean:
	rm -rf $(VENV)

lint:
	python -m uv pip install --quiet --upgrade pycln isort ruff
	python -m ruff check --fix --exit-zero
	python -m pycln .
	python -m isort .
	python -m ruff format src