all: test

RABBITMQ_CONTAINER_NAME:=aio_pika_rabbitmq
RABBITMQ_IMAGE:=mosquito/aiormq-rabbitmq

test:
	find . -name "*.pyc" -type f -delete
	tox

rabbitmq:
	docker pull $(RABBITMQ_IMAGE)
	docker kill $(RABBITMQ_CONTAINER_NAME) || true
	docker run --rm -d \
		--name $(RABBITMQ_CONTAINER_NAME) \
		-p 5671:5671 \
		-p 5672:5672 \
		-p 15671:15671 \
		-p 15672:15672 \
		$(RABBITMQ_IMAGE)

upload:
	python3.7 setup.py sdist bdist_wheel
	twine upload dist/*$(shell python3 setup.py --version)*
