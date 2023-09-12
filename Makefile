include .env
export

setup-local:
	pipenv shell &&\
	pipenv install &&\
	localstack start -d &&\
	awslocal s3 mb s3://$(S3_BUCKET)

test:
	pytest

clean-files:
	isort --sp .github/linters/.isort.cfg
	black .
	flake8 --config .github/linters/.flake8

run-docker:
	docker build . -t $(PROJECT)
	docker run --rm $(PROJECT)
	docker rmi $(PROJECT)