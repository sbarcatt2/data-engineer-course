IMAGE_NAME=curso_data_engineer

build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run --rm -it $(IMAGE_NAME)

shell:
	docker run --rm -it $(IMAGE_NAME) /bin/bash

rebuild: clean build

clean:
	docker rmi $(IMAGE_NAME) || true