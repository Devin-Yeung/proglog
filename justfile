dev-up:
    go run ./cmd/server \
      --cert-file=${PROGLOG_CONFIG_DIR}/server.pem \
      --key-file=${PROGLOG_CONFIG_DIR}/server-key.pem \
      --ca-file=${PROGLOG_CONFIG_DIR}/ca.pem \
      --acl-model-file=${PROGLOG_CONFIG_DIR}/model.conf \
      --acl-policy-file=${PROGLOG_CONFIG_DIR}/policy.csv \
      --data-dir="/tmp/proglog"

test:
  gotestsum --format=testname

setup: gencert gen-acl-rules

compile:
  protoc api/v1/*.proto \
    --go_out=. \
    --go-grpc_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_opt=paths=source_relative \
    --proto_path=.

gen-acl-rules:
    cp test/model.conf ${PROGLOG_CONFIG_DIR}
    cp test/policy.csv ${PROGLOG_CONFIG_DIR}

gencert:
  cfssl gencert \
    -initca test/ca-csr.json \
    | cfssljson -bare ca

  cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=test/ca-config.json \
    -profile=server \
    test/server-csr.json \
    | cfssljson -bare server

  # root client is the superuser
  cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=test/ca-config.json \
    -profile=client \
    -cn=root \
    test/client-csr.json \
    | cfssljson -bare root-client

  # nobody client is the un-privileged user
  cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=test/ca-config.json \
    -profile=client \
    -cn=nobody \
    test/client-csr.json \
    | cfssljson -bare nobody-client


  mkdir -p ${PROGLOG_CONFIG_DIR}
  mv *.pem *.csr ${PROGLOG_CONFIG_DIR}
