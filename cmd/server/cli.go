package main

type Config struct {
	ListenHost    string `name:"listen-host" help:"Host or interface to bind." default:"127.0.0.1"`
	ListenPort    int    `name:"listen-port" help:"TCP port to listen on." default:"8400"`
	DataDir       string `name:"data-dir" help:"Directory for commit log data." default:"/tmp/proglog"`
	CertFile      string `name:"cert-file" help:"Path to server TLS certificate PEM." env:"PROGLOG_SERVER_CERT_FILE" required:""`
	KeyFile       string `name:"key-file" help:"Path to server TLS key PEM." env:"PROGLOG_SERVER_KEY_FILE" required:""`
	CAFile        string `name:"ca-file" help:"Path to CA certificate PEM." env:"PROGLOG_CA_FILE" required:""`
	ACLModelFile  string `name:"acl-model-file" help:"Path to ACL model.conf file." env:"PROGLOG_ACL_MODEL_FILE" required:""`
	ACLPolicyFile string `name:"acl-policy-file" help:"Path to ACL policy.csv file." env:"PROGLOG_ACL_POLICY_FILE" required:""`
}
