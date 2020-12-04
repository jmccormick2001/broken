SAMPLE_WATCH_DIRS=/churro
GRPC_CERTS_DIR=certs/grpc
DB_CERTS_DIR=certs/db
BUILDDIR=./build
PIPELINE=pipeline4
CHURRO_NS=churro
.DEFAULT_GOAL := all

setup-env:
	echo 'downloading build dependencies...'
	echo be sure to add $(HOME)/go/bin to your PATH
	go get -u github.com/golang/protobuf/protoc-gen-go
	wget https://github.com/protocolbuffers/protobuf/releases/download/v3.12.4/protoc-3.12.4-linux-x86_64.zip
	unzip protoc-3.12.4-linux-x86_64.zip 'bin/protoc' -d $(HOME)/go
	go get -u go101.org/gold
	which protoc-gen-go

## Create certificates for a pipeline
cert:
	$(BUILDDIR)/gen-certs.sh certs $(PIPELINE)
gen-docs:
	echo 'generating docs...'
	gold -gen -dir=/tmp ./...
unit-test:
	echo 'unit tests...'
	go test ./cmd/... ./internal... -v
deploy-operator:
	build/namespace-check.sh $(CHURRO_NS)
	kubectl delete crd pipelines.churro.project.io --ignore-not-found=true
	kubectl -n $(CHURRO_NS) delete pod churro-operator --ignore-not-found=true
	kubectl -n $(CHURRO_NS) delete clusterrole churro-operator --ignore-not-found=true
	kubectl -n $(CHURRO_NS) delete clusterrolebinding churro-operator --ignore-not-found=true
	kubectl -n $(CHURRO_NS) delete serviceaccount churro-operator --ignore-not-found=true
	kubectl -n $(CHURRO_NS) delete configmap churro-templates --ignore-not-found=true
	kubectl -n $(CHURRO_NS) create configmap churro-templates --from-file=deploy/templates
	kubectl -n $(CHURRO_NS) create -f deploy/operator/churro.project.io_pipelines.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/operator/cluster-role.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/operator/cluster-role-binding.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/operator/service-account.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/operator/churro-operator.yaml
	kubectl -n $(CHURRO_NS) delete --ignore-not-found=true pod/churro-ui \
		pvc/churro-admindb service/churro-ui clusterrole/churro-ui \
		clusterrolebinding/churro-ui 
	kubectl -n $(CHURRO_NS) create -f deploy/ui/admindb-pvc.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/ui/service-account.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/ui/cluster-role.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/ui/cluster-role-binding.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/ui/service.yaml
	kubectl -n $(CHURRO_NS) create -f deploy/ui/churro-ui.yaml
push:
	docker push registry.gitlab.com/churro-group/churro/churro-extract
	docker push registry.gitlab.com/churro-group/churro/churro-loader
	docker push registry.gitlab.com/churro-group/churro/churro-watch
	docker push registry.gitlab.com/churro-group/churro/churro-ctl
	docker push registry.gitlab.com/churro-group/churro/churro-operator
push-kind:
	kind load docker-image registry.gitlab.com/churro-group/churro/churro-extract
	kind load docker-image registry.gitlab.com/churro-group/churro/churro-loader
	kind load docker-image registry.gitlab.com/churro-group/churro/churro-watch
	kind load docker-image registry.gitlab.com/churro-group/churro/churro-ctl
	kind load docker-image registry.gitlab.com/churro-group/churro/churro-operator
	kind load docker-image registry.gitlab.com/churro-group/churro/churro-ui

compile-ui:
	go build -o build/churro-ui ui/main.go

build-ui: compile-ui
	# compile it statically for including into the alpine image
	#CGO_ENABLED=1 GOOS=linux go build -a -o build/churro-ui ui/main.go
#	docker rmi registry.gitlab.com/churro-group/churro/churro-ui:latest
	docker build -f ./images/Dockerfile.churro-ui -t registry.gitlab.com/churro-group/churro/churro-ui .

compile-extract:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=require_unimplemented_servers=false:. --go-grpc_opt=paths=source_relative rpc/extract/extract-service.proto
	#protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative rpc/extract/extract-service.proto
	go build -o build/churro-extract cmd/churro-extract/churro-extract.go
build-extract: compile-extract
	# compile it statically for including into the alpine image
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags='-extldflags "-static"' -o build/churro-extract cmd/churro-extract/churro-extract.go
	docker build -f ./images/Dockerfile.churro-extract -t registry.gitlab.com/churro-group/churro/churro-extract .

compile-loader:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=require_unimplemented_servers=false:. --go-grpc_opt=paths=source_relative rpc/loader/loader-service.proto
	#protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative rpc/loader/loader-service.proto
	go build -o build/churro-loader cmd/churro-loader/churro-loader.go
build-loader: compile-loader
	# compile it statically for including into the alpine image
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags='-extldflags "-static"' -o build/churro-loader cmd/churro-loader/churro-loader.go
	docker build -f ./images/Dockerfile.churro-loader -t registry.gitlab.com/churro-group/churro/churro-loader .

compile-watch:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=require_unimplemented_servers=false:. --go-grpc_opt=paths=source_relative rpc/watch/watch-service.proto
	#protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative rpc/watch/watch-service.proto
	go build -o build/churro-watch cmd/churro-watch/churro-watch.go
build-watch: compile-watch
	# compile it statically for including into the alpine image
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags='-extldflags "-static"' -o build/churro-watch cmd/churro-watch/churro-watch.go
	docker build -f ./images/Dockerfile.churro-watch -t registry.gitlab.com/churro-group/churro/churro-watch .

compile-ctl:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=require_unimplemented_servers=false:. --go-grpc_opt=paths=source_relative rpc/ctl/ctl-service.proto
	#protoc --go_out=plugins=grpc:. --go_opt=paths=source_relative rpc/ctl/ctl-service.proto
	go build -o build/churro-ctl cmd/churro-ctl/churro-ctl.go
build-ctl: compile-ctl
	# compile it statically for including into the alpine image
#	CGO_ENABLED=0 GOOS=linux go build -a -ldflags='-extldflags "-static"' -o build/churro-ctl cmd/churro-ctl/churro-ctl.go
	docker build -f ./images/Dockerfile.churro-ctl -t registry.gitlab.com/churro-group/churro/churro-ctl .

compile-operator:
	go build -o build/churro-operator cmd/churro-operator/churro-operator.go
build-operator: compile-operator
	# compile it statically for including into the alpine image
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags='-extldflags "-static"' -o build/churro-operator cmd/churro-operator/churro-operator.go
	docker build -f ./images/Dockerfile.churro-operator -t registry.gitlab.com/churro-group/churro/churro-operator .

compile-churroctl:
	go build -o $(BUILDDIR)/churroctl cmd/churroctl/main.go

churroctl-port-forward:
	kubectl -n $(PIPELINE) port-forward svc/churro-ctl 8088:8088 
churroctl-get:
	$(BUILDDIR)/churroctl get pipeline fuzzy \
		--namespace=fuzzy \
		--url 127.0.0.1:8088
churroctl-create:
	$(BUILDDIR)/churroctl create pipeline fuzzy \
		--cr=./deploy/fuzzy/churro.project.io_v1alpha1_pipeline_cr.yaml \
		--url 127.0.0.1:8088

compile: compile-operator compile-ctl compile-loader compile-watch compile-extract compile-churroctl

run-operator-local:
	cp deploy/templates/* /templates/
	go run cmd/churro-operator/main.go -db-creds-dir certs/db/somepipeline -grpc-creds-dir certs/grpc
run-ui:
	./ui/run-web

all: build-extract build-loader build-watch build-ctl build-operator build-ui

pipeline-certs:
	$(BUILDDIR)/gen-certs.sh certs $(PIPELINE)
somepipeline:
	kubectl delete namespace somepipeline --ignore-not-found=true
	kubectl create namespace somepipeline
	kubectl create -n somepipeline -f deploy/somepipeline/churro.project.io_v1alpha1_pipeline_cr.yaml

get-pipeline:
	$(BUILDDIR)/churroctl create pipeline --servicecrt=certs/grpc/service.pem  --config=test/service-configs/churro-config.yaml

create-pipeline: delete-pipeline
	# create db and grpc secrets
	kubectl -n $(PIPELINE) create secret generic cockroachdb.client.root \
	--from-file=$(DB_CERTS_DIR)
	kubectl -n $(PIPELINE) create secret generic cockroachdb.node \
	--from-file=$(DB_CERTS_DIR)
	kubectl -n $(PIPELINE) create secret generic pipeline1.client.root \
	--from-file=$(GRPC_CERTS_DIR)
	kubectl -n $(PIPELINE) create secret generic pipeline1.config \
	--from-file=test/service-configs
	# start db
	kubectl -n $(PIPELINE) create -f deploy/cockroach/cockroachdb-statefulset.yaml
	echo "sleeping 25 seconds for cockroachdb to come up..."
	sleep 25
	kubectl -n $(PIPELINE) exec -it cockroachdb-2 -- /cockroach/cockroach init --certs-dir=/cockroach/cockroach-certs
	kubectl -n $(PIPELINE) create -f ./deploy/pipeline1/churro-role.yaml
	kubectl -n $(PIPELINE) create -f ./deploy/pipeline1/churro-role-binding.yaml
	kubectl -n $(PIPELINE) create -f ./deploy/pipeline1/churro-service-account.yaml
	sleep 5
	kubectl -n $(PIPELINE) create -f deploy/cockroach/client.yaml
	echo 'init pipeline1 ...'
	kubectl -n $(PIPELINE) create -f ./deploy/pipeline1/churrodata-pvc.yaml
	kubectl -n $(PIPELINE) create -f ./deploy/pipeline1/churro-ctl.yaml
	sleep 5
	echo "sleeping 15 seconds, in another terminal window run....kubectl -n $(PIPELINE) port-forward pod/churro-ctl 8088:8088"
	sleep 15
	$(BUILDDIR)/churroctl create pipeline --servicecrt=certs/grpc/service.pem  --config=test/service-configs/churro-config.yaml
	sleep 5
	kubectl -n $(PIPELINE) create -f ./deploy/pipeline1/churro-watch.yaml
	kubectl -n $(PIPELINE) create -f ./deploy/pipeline1/churro-loader.yaml
	kubectl -n $(PIPELINE) create -f ./deploy/cockroach/client.yaml


delete-pipeline:
	kubectl -n $(PIPELINE) delete secret/cockroachdb.client.root \
	secret/cockroachdb.node --ignore-not-found=true
	kubectl -n $(PIPELINE) delete secret/pipeline1.client.root \
	secret/pipeline1.config --ignore-not-found=true
	kubectl -n $(PIPELINE) delete role/churro rolebinding/churro \
	serviceaccount/churro --ignore-not-found=true
	kubectl -n $(PIPELINE) delete pod/churro-ctl pod/churro-watch pod/churro-loader --ignore-not-found=true
	kubectl -n $(PIPELINE) delete pod/cockroachdb-client-secure --ignore-not-found=true
	kubectl -n $(PIPELINE) delete statefulset/cockroachdb --ignore-not-found=true
	kubectl -n $(PIPELINE) delete sa/cockroachdb sa/churro --ignore-not-found=true
	kubectl -n $(PIPELINE) delete pvc --all --ignore-not-found=true
	kubectl -n $(PIPELINE) delete secret --selector=pipeline=pipeline1 --ignore-not-found=true
	kubectl -n $(PIPELINE) delete secret/pipeline1.client.root secret/pipeline1.config --ignore-not-found=true
	kubectl -n $(PIPELINE) delete service/cockroachdb-public \
	service/cockroachdb --ignore-not-found=true
	kubectl -n $(PIPELINE) delete pdb/cockroachdb-budget --ignore-not-found=true
	kubectl -n $(PIPELINE) delete role/cockroachdb \
	rolebinding/cockroachdb --ignore-not-found=true
	kubectl -n $(PIPELINE) delete service/churro-ctl \
	service/churro-watch service/churro-loader --ignore-not-found=true

gen-godoc-site:
	godoc-static \
		-site-name="churro Documentation" \
		-destination=./doc/godoc \
		gitlab.com/churro-group/churro/internal/backpressure 
start-prometheus:
	./deploy/prometheus/start-prometheus.sh
run-db-client:
	kubectl -n $(PIPELINE) exec -it cockroachdb-client-secure -- ./cockroach sql --certs-dir=/cockroach-certs --host=cockroachdb-public
.PHONY: clean

clean:
	rm $(BUILDDIR)/churro*
	rm /tmp/churro*.log
