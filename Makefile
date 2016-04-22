clean:
	# Remove the build
	sudo rm -rf build dist
	# And all of our pyc files
	find . -name '*.pyc' | xargs -n 100 rm
	# And lastly, .coverage files
	find . -name .coverage | xargs rm

.PHONY: test
test:
	rm -f .coverage
	nosetests --exe --cover-package=nsq --with-coverage --cover-branches -v --logging-clear-handlers

requirements:
	pip freeze | grep -v -e nsq-py > requirements.txt
