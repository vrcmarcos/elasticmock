install:
	@pip install -r requirements.txt

test_install: install
	@pip install -r requirements_test.txt

test: test_install
	@tox -p 20 --parallel--safe-build

upload: create_dist
	@pip install twine
	@twine upload dist/*

create_dist: update_pip
	@rm -rf dist
	@python setup.py sdist

update_pip:
	@pip install --upgrade pip
