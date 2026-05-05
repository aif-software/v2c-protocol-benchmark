set -e
CONFIG_FILE="../client/benchmarking/benchmark_config.json"
SERVER_HOST=$(jq -r '.client_settings.server_address' "$CONFIG_FILE")

# CA
openssl genrsa -out ca.key 4096

openssl req -x509 -new -nodes \
  -key ca.key \
  -sha256 -days 3650 \
  -out ca.crt \
  -subj "/CN=MyCA"

# server key
openssl genrsa -out server.key 2048

# SAN config
cat > san.cnf <<EOF
subjectAltName=IP:$SERVER_HOST
EOF

# CSR
openssl req -new \
  -key server.key \
  -out server.csr \
  -subj "/CN=$SERVER_HOST"

# Sign cert
openssl x509 -req \
  -in server.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -out server.crt \
  -days 365 \
  -sha256 \
  -extfile san.cnf