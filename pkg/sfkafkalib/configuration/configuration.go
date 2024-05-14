package configuration

type ConsumerConfig struct {
	System                   string
	Topic                    string
	Application              string
	SchemaInfoPath           string
	SchemaCredsPath          string
	VaultIkeyPath            string
	VaultPath                string
	TraceInstrumentationName string
}
