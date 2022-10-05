# makefile used for testing
#
#
all: install test

install:
	python3 -m pip install -e .[tests,docs]

test:
	python3 -m pytest -vv

lint:
	mypy --install-types --non-interactive
	black --check -t py310 -l 79 src
	flake8 src/metadata_inspector --count --max-complexity=15 --max-line-length=88 --statistics --doctests
