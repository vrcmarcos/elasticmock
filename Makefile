install:
	@pip install -r requirements.txt

test_install: install
	@pip install -r requirements_test.txt

test: test_install
	@py.test --cov-report term-missing --cov=elasticmock

upload:
	@python setup.py sdist upload