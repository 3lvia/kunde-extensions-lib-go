package salesforce

type KafkaMessage__c struct {
	Key__c   string
	Value__c []byte
	Topic__c string
}
