all: test

test:
	find . -name "*.pyc" -type f -delete
	tox
