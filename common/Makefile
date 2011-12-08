sdist: 
	python setup.py sdist

rpm:
	rpmbuild -ba --define "_sourcedir ${PWD}/dist" batchacct-common.spec
