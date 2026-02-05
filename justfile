test:
  gotestsum --format=testname

setup: gencert

compile:
  protoc api/v1/*.proto \
    --go_out=. \
    --go-grpc_out=. \
    --go_opt=paths=source_relative \
    --go-grpc_opt=paths=source_relative \
    --proto_path=.

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

  cfssl gencert \
    -ca=ca.pem \
    -ca-key=ca-key.pem \
    -config=test/ca-config.json \
    -profile=client \
    test/client-csr.json \
    | cfssljson -bare client

  mkdir -p ${PROGLOG_CONFIG_DIR}
  mv *.pem *.csr ${PROGLOG_CONFIG_DIR}
