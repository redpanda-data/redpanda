package certmanager

import redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"

// Helper functions and types for Listeners

var (
	_ APIListener = redpandav1alpha1.KafkaAPI{}
	_ APIListener = redpandav1alpha1.AdminAPI{}
	_ APIListener = redpandav1alpha1.PandaproxyAPI{}
	_ APIListener = redpandav1alpha1.SchemaRegistryAPI{}
)

// APIListener is a generic API Listener
type APIListener interface {
	// GetPort returns API port
	GetPort() int
	// GetTLS returns API TLSConfig
	GetTLS() *redpandav1alpha1.TLSConfig
	// GetExternal returns API's ExternalConnectivityConfig
	GetExternal() *redpandav1alpha1.ExternalConnectivityConfig
}

func kafkaAPIListeners(r *redpandav1alpha1.Cluster) []APIListener {
	listeners := []APIListener{}
	for _, el := range r.Spec.Configuration.KafkaAPI {
		listeners = append(listeners, el)
	}
	return listeners
}

func adminAPIListeners(r *redpandav1alpha1.Cluster) []APIListener {
	listeners := []APIListener{}
	for _, el := range r.Spec.Configuration.AdminAPI {
		listeners = append(listeners, el)
	}
	return listeners
}

func schemaRegistryAPIListeners(r *redpandav1alpha1.Cluster) []APIListener {
	if r.Spec.Configuration.SchemaRegistry == nil {
		return []APIListener{}
	}

	return []APIListener{*r.Spec.Configuration.SchemaRegistry}
}

// PandaProxyAPIListeners returns all PandaProxyAPI listeners
func pandaProxyAPIListeners(r *redpandav1alpha1.Cluster) []APIListener {
	listeners := []APIListener{}
	for _, el := range r.Spec.Configuration.PandaproxyAPI {
		listeners = append(listeners, el)
	}
	return listeners
}

func getExternalTLSListener(listeners []APIListener) APIListener {
	tlsListener := getTLSListener(listeners)
	if tlsListener == nil {
		return nil
	}
	if ext := tlsListener.GetExternal(); ext != nil && ext.Enabled {
		return tlsListener
	}
	return nil
}

func getInternalTLSListener(listeners []APIListener) APIListener {
	tlsListener := getTLSListener(listeners)
	if tlsListener == nil {
		return nil
	}
	if ext := tlsListener.GetExternal(); ext == nil || !ext.Enabled {
		return tlsListener
	}
	return nil
}

func getTLSListener(listeners []APIListener) APIListener {
	for _, el := range listeners {
		tls := el.GetTLS()
		if tls != nil && tls.Enabled {
			return el
		}
	}
	return nil
}
