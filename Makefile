all: test

RABBITMQ_IMAGE:=mosquito/aiormq-rabbitmq

test:
	find . -name "*.pyc" -type f -delete
	tox

rabbitmq:
	docker kill $(docker ps -f label=aio-pika.rabbitmq -q) || true
	docker pull $(RABBITMQ_IMAGE)
	docker run --rm -d \
		-l aio-pika.rabbitmq \
		-p 5671:5671 \
		-p 5672:5672 \
		-p 15671:15671 \
		-p 15672:15672 \
		$(RABBITMQ_IMAGE)

upload:
	python3.7 setup.py sdist bdist_wheel
	twine upload dist/*$(shell python3 setup.py --version)*
