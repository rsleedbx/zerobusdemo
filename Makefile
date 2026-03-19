.PHONY: venv test test-unit test-integration clean

VENV      := .venv
PYTHON    := $(VENV)/bin/python
PIP       := $(VENV)/bin/pip
PYTEST    := $(VENV)/bin/pytest

## Create virtual environment and install dependencies
venv:
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

## Run all unit tests (no live credentials needed)
test:
	$(PYTEST) tests/ -v --tb=short -k "not Integration"

## Run only unit tests
test-unit:
	$(PYTEST) tests/test_protobuf_converter.py tests/test_zerobus_ingest.py -v --tb=short

## Run integration tests (requires ZEROBUS_TABLE_NAME and ~/.databrickscfg)
test-integration:
	$(PYTEST) tests/ -v --tb=short -k "Integration"

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache src/__pycache__ tests/__pycache__
