install:
	@pip install -r requirements.txt

test_install: install
	@pip install -r requirements_test.txt

test: test_install
	@tox -p 20 --parallel--safe-build

upload: create_dist
	@twine upload dist/*

create_dist: install_twine
	@python setup.py sdist

install_twine: update_pip
	@pip install twine

update_pip:
	@pip install --upgrade pip
