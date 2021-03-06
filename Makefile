ELASTICMOCK_VERSION='1.8.0'

install:
	pip3 install -r requirements.txt

test_install: install
	pip3 install -r requirements_test.txt

test: test_install
	python3.8 setup.py test

upload: create_dist
	pip3 install twine
	twine upload dist/*
	git push

create_dist: create_dist_no_commit update_pip
	rm -rf dist
	python3.8 setup.py sdist

create_dist_no_commit: update_pip
	rm -rf dist
	python3.8 setup.py sdist

create_dist_commit:
	git commit --all -m "Bump version ${ELASTICMOCK_VERSION}"
	git tag ${ELASTICMOCK_VERSION}

update_pip:
	pip3 install --upgrade pip
