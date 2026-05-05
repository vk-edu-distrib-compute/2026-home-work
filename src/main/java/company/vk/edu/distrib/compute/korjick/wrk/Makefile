WRK_IMAGE ?= haydenjeune/wrk2:latest
HOST ?= host.docker.internal
PORT ?= 8080
URL ?= http://$(HOST):$(PORT)
RATE ?= 200
DURATION ?= 30s
KEY_SPACE ?= 100000
VALUE_SIZE ?= 128

SINGLE_THREADS ?= 1
SINGLE_CONNECTIONS ?= 1
CLUSTER_THREADS ?= 2
CLUSTER_CONNECTIONS ?= 100

REPLICA_LOCAL_ACK ?= 1
REPLICA_QUORUM_ACK ?= 3

.PHONY: stress-single stress-replica stress-replica-local stress-replica-quorum stress-cluster

stress-single:
	docker run --rm -v "$(CURDIR)/put.lua:/data/request.lua" -e KEY_SPACE=$(KEY_SPACE) -e VALUE_SIZE=$(VALUE_SIZE) $(WRK_IMAGE) -t$(SINGLE_THREADS) -c$(SINGLE_CONNECTIONS) -R$(RATE) -d$(DURATION) --latency -s /data/request.lua $(URL)
	docker run --rm -v "$(CURDIR)/get.lua:/data/request.lua" -e KEY_SPACE=$(KEY_SPACE) $(WRK_IMAGE) -t$(SINGLE_THREADS) -c$(SINGLE_CONNECTIONS) -R$(RATE) -d$(DURATION) --latency -s /data/request.lua $(URL)

stress-replica: stress-replica-local stress-replica-quorum

stress-replica-local:
	docker run --rm -v "$(CURDIR)/put.lua:/data/request.lua" -e ACK=$(REPLICA_LOCAL_ACK) -e KEY_SPACE=$(KEY_SPACE) -e VALUE_SIZE=$(VALUE_SIZE) $(WRK_IMAGE) -t$(SINGLE_THREADS) -c$(SINGLE_CONNECTIONS) -R$(RATE) -d$(DURATION) --latency -s /data/request.lua $(URL)
	docker run --rm -v "$(CURDIR)/get.lua:/data/request.lua" -e ACK=$(REPLICA_LOCAL_ACK) -e KEY_SPACE=$(KEY_SPACE) $(WRK_IMAGE) -t$(SINGLE_THREADS) -c$(SINGLE_CONNECTIONS) -R$(RATE) -d$(DURATION) --latency -s /data/request.lua $(URL)

stress-replica-quorum:
	docker run --rm -v "$(CURDIR)/put.lua:/data/request.lua" -e ACK=$(REPLICA_QUORUM_ACK) -e KEY_SPACE=$(KEY_SPACE) -e VALUE_SIZE=$(VALUE_SIZE) $(WRK_IMAGE) -t$(SINGLE_THREADS) -c$(SINGLE_CONNECTIONS) -R$(RATE) -d$(DURATION) --latency -s /data/request.lua $(URL)
	docker run --rm -v "$(CURDIR)/get.lua:/data/request.lua" -e ACK=$(REPLICA_QUORUM_ACK) -e KEY_SPACE=$(KEY_SPACE) $(WRK_IMAGE) -t$(SINGLE_THREADS) -c$(SINGLE_CONNECTIONS) -R$(RATE) -d$(DURATION) --latency -s /data/request.lua $(URL)

stress-cluster:
	docker run --rm -v "$(CURDIR)/put.lua:/data/request.lua" -e KEY_SPACE=$(KEY_SPACE) -e VALUE_SIZE=$(VALUE_SIZE) $(WRK_IMAGE) -t$(CLUSTER_THREADS) -c$(CLUSTER_CONNECTIONS) -R$(RATE) -d$(DURATION) --latency -s /data/request.lua $(URL)
	docker run --rm -v "$(CURDIR)/get.lua:/data/request.lua" -e KEY_SPACE=$(KEY_SPACE) $(WRK_IMAGE) -t$(CLUSTER_THREADS) -c$(CLUSTER_CONNECTIONS) -R$(RATE) -d$(DURATION) --latency -s /data/request.lua $(URL)
