VERSION = 0.7.0-1
IMAGE = skale/skale

all:

image:
	docker build -t $(IMAGE):$(VERSION) .
	docker tag $(IMAGE):$(VERSION) $(IMAGE):latest

rmi:
	docker rmi $(IMAGE):$(VERSION) $(IMAGE):latest

test:
	docker run --rm -ti $(IMAGE) sh

# Publish only if you are image owner
publish:
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest
