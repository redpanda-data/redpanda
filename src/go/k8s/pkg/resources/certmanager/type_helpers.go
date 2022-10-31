// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package certmanager

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	cmmetav1 "github.com/jetstack/cert-manager/pkg/apis/meta/v1"
	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	resourcetypes "github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/types"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	redpandaCertVolName       = "tlscert"
	redpandaCAVolName         = "tlsca"
	adminAPICertVolName       = "tlsadmincert"
	adminAPICAVolName         = "tlsadminca"
	pandaProxyCertVolName     = "tlspandaproxycert"
	pandaProxyCAVolName       = "tlspandaproxyca"
	schemaRegistryCertVolName = "tlsschemaregistrycert"
	schemaRegistryCAVolName   = "tlsschemaregistryca"
)

// Helper functions and types for Listeners

var (
	_ APIListener = redpandav1alpha1.KafkaAPI{}
	_ APIListener = redpandav1alpha1.AdminAPI{}
	_ APIListener = redpandav1alpha1.PandaproxyAPI{}
	_ APIListener = redpandav1alpha1.SchemaRegistryAPI{}

	errNoTLSError = errors.New("no TLS enabled for admin API")
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
	pp := r.Spec.Configuration.PandaproxyAPI
	for i := range r.Spec.Configuration.PandaproxyAPI {
		listeners = append(listeners, pp[i])
	}
	return listeners
}

func getExternalTLSListener(listeners []APIListener) APIListener {
	tlsListeners := getTLSListeners(listeners)
	for _, l := range tlsListeners {
		if ext := l.GetExternal(); ext != nil && ext.Enabled {
			return l
		}
	}
	return nil
}

func getInternalTLSListener(listeners []APIListener) APIListener {
	tlsListeners := getTLSListeners(listeners)
	for _, l := range tlsListeners {
		if ext := l.GetExternal(); ext == nil || !ext.Enabled {
			return l
		}
	}
	return nil
}

func getTLSListeners(listeners []APIListener) []APIListener {
	res := []APIListener{}
	for i, el := range listeners {
		tlsConfig := el.GetTLS()
		if tlsConfig != nil && tlsConfig.Enabled {
			res = append(res, listeners[i])
		}
	}
	return res
}

// apiCertificates is a collection of certificate resources per single API. It
// contains node and client certificates (if mutual TLS is enabled)
type apiCertificates struct {
	nodeCertificate    resources.Resource
	clientCertificates []resources.Resource
	rootResources      []resources.Resource
	tlsEnabled         bool

	// CR allows to specify node certificate, if not provided this will be nil
	externalNodeCertificate *corev1.ObjectReference

	// all certificates need to exist in this namespace for mounting of secrets to work
	clusterNamespace string
}

func tlsDisabledAPICertificates() *apiCertificates {
	return &apiCertificates{
		tlsEnabled: false,
	}
}

func tlsEnabledAPICertificates(namespace string) *apiCertificates {
	return &apiCertificates{
		tlsEnabled:       true,
		clusterNamespace: namespace,
	}
}

// ClusterCertificates contains definition for all resources needed to be
// created to support TLS on all redpanda APIs where TLS is enabled
type ClusterCertificates struct {
	// certificates to be created for these APIs. We currently create different
	// set of node and client certificates per API
	kafkaAPI          *apiCertificates
	schemaRegistryAPI *apiCertificates
	adminAPI          *apiCertificates
	pandaProxyAPI     *apiCertificates

	client       client.Client
	scheme       *runtime.Scheme
	pandaCluster *redpandav1alpha1.Cluster
	internalFQDN string
	clusterFQDN  string
	logger       logr.Logger
}

// NewClusterCertificates creates new cluster tls certificates resources
func NewClusterCertificates(
	cluster *redpandav1alpha1.Cluster,
	keystoreSecret types.NamespacedName,
	k8sClient client.Client,
	fqdn string,
	clusterFQDN string,
	scheme *runtime.Scheme,
	logger logr.Logger,
) *ClusterCertificates {
	cc := &ClusterCertificates{
		pandaCluster: cluster,
		client:       k8sClient,
		scheme:       scheme,
		internalFQDN: fqdn,
		clusterFQDN:  clusterFQDN,
		logger:       logger,

		kafkaAPI:          tlsDisabledAPICertificates(),
		schemaRegistryAPI: tlsDisabledAPICertificates(),
		adminAPI:          tlsDisabledAPICertificates(),
		pandaProxyAPI:     tlsDisabledAPICertificates(),
	}
	if kafkaListeners := kafkaAPIListeners(cluster); len(kafkaListeners) > 0 {
		cc.kafkaAPI = cc.prepareAPI(kafkaAPI, RedpandaNodeCert, []string{OperatorClientCert, UserClientCert, AdminClientCert}, kafkaListeners, &keystoreSecret)
	}

	if adminListeners := adminAPIListeners(cluster); len(adminListeners) > 0 {
		cc.adminAPI = cc.prepareAPI(adminAPI, adminAPINodeCert, []string{adminAPIClientCert}, adminListeners, &keystoreSecret)
	}

	if pandaProxyListeners := pandaProxyAPIListeners(cluster); len(pandaProxyListeners) > 0 {
		cc.pandaProxyAPI = cc.prepareAPI(pandaproxyAPI, pandaproxyAPINodeCert, []string{pandaproxyAPIClientCert}, pandaProxyListeners, &keystoreSecret)
	}

	if schemaRegistryListeners := schemaRegistryAPIListeners(cluster); len(schemaRegistryListeners) > 0 {
		cc.schemaRegistryAPI = cc.prepareAPI(schemaRegistryAPI, schemaRegistryAPINodeCert, []string{schemaRegistryAPIClientCert}, schemaRegistryListeners, &keystoreSecret)
	}

	return cc
}

func (cc *ClusterCertificates) prepareAPI(
	rootCertSuffix string,
	nodeCertSuffix string,
	clientCerts []string,
	listeners []APIListener,
	keystoreSecret *types.NamespacedName,
) *apiCertificates {
	tlsListeners := getTLSListeners(listeners)
	externalTLSListener := getExternalTLSListener(listeners)
	internalTLSListener := getInternalTLSListener(listeners)

	if len(tlsListeners) == 0 {
		return tlsDisabledAPICertificates()
	}
	result := tlsEnabledAPICertificates(cc.pandaCluster.Namespace)

	// TODO(#3550): Do not create rootIssuer if nodeSecretRef is passed and mTLS is disabled
	toApplyRoot, rootIssuerRef := prepareRoot(rootCertSuffix, cc.client, cc.pandaCluster, cc.scheme, cc.logger)
	result.rootResources = toApplyRoot
	nodeIssuerRef := rootIssuerRef

	// for now we disallow having different issuer for each listener so that
	// every time both listeners share the same set of certificates
	if tlsListeners[0].GetTLS().IssuerRef != nil {
		// if external issuer is provided, we will use it to generate node certificates
		nodeIssuerRef = tlsListeners[0].GetTLS().IssuerRef
	}

	// for now we disallow having different issuer for each listener so that
	// every time both listeners share the same set of certificates
	nodeSecretRef := tlsListeners[0].GetTLS().NodeSecretRef
	result.externalNodeCertificate = nodeSecretRef
	if nodeSecretRef == nil || nodeSecretRef.Name == "" {
		certName := NewCertName(cc.pandaCluster.Name, nodeCertSuffix)
		certsKey := types.NamespacedName{Name: string(certName), Namespace: cc.pandaCluster.Namespace}
		dnsNames := []string{}

		if internalTLSListener != nil {
			dnsNames = append(dnsNames, cc.clusterFQDN, cc.internalFQDN)
		}
		// TODO(#2256): Add support for external listener + TLS certs for IPs
		if externalTLSListener != nil && externalTLSListener.GetExternal().Subdomain != "" {
			dnsNames = append(dnsNames, externalTLSListener.GetExternal().Subdomain)
		}

		nodeCert := NewNodeCertificate(
			cc.client,
			cc.scheme,
			cc.pandaCluster,
			certsKey,
			nodeIssuerRef,
			dnsNames,
			EmptyCommonName,
			keystoreSecret,
			cc.logger)
		result.nodeCertificate = nodeCert
	}

	anyListenerWithMutualTLS := false
	for _, l := range tlsListeners {
		if l.GetTLS().RequireClientAuth {
			anyListenerWithMutualTLS = true
		}
	}
	if anyListenerWithMutualTLS {
		// if there is at least one listener with mutual tls, we are going to
		// generate the client certificates
		for _, clientCertName := range clientCerts {
			clientCn := NewCommonName(cc.pandaCluster.Name, clientCertName)
			clientKey := types.NamespacedName{Name: string(clientCn), Namespace: cc.pandaCluster.Namespace}
			clientCert := NewCertificate(cc.client, cc.scheme, cc.pandaCluster, clientKey, rootIssuerRef, clientCn, false, keystoreSecret, cc.logger)
			result.clientCertificates = append(result.clientCertificates, clientCert)
		}
	}

	return result
}

func prepareRoot(
	prefix string,
	k8sClient client.Client,
	pandaCluster *redpandav1alpha1.Cluster,
	scheme *runtime.Scheme,
	logger logr.Logger,
) ([]resources.Resource, *cmmetav1.ObjectReference) {
	toApply := []resources.Resource{}
	selfSignedIssuer := NewIssuer(k8sClient,
		scheme,
		pandaCluster,
		issuerNamespacedName(pandaCluster.Name, pandaCluster.Namespace, prefix+"-"+"selfsigned-issuer"),
		"",
		logger)

	rootCn := NewCommonName(pandaCluster.Name, prefix+"-root-certificate")
	rootKey := types.NamespacedName{Name: string(rootCn), Namespace: pandaCluster.Namespace}
	rootCertificate := NewCACertificate(k8sClient,
		scheme,
		pandaCluster,
		rootKey,
		selfSignedIssuer.objRef(),
		rootCn,
		nil,
		logger)

	leafIssuer := NewIssuer(k8sClient,
		scheme,
		pandaCluster,
		issuerNamespacedName(pandaCluster.Name, pandaCluster.Namespace, prefix+"-"+"root-issuer"),
		rootCertificate.Key().Name,
		logger)

	leafIssuerRef := leafIssuer.objRef()

	toApply = append(toApply, selfSignedIssuer, rootCertificate, leafIssuer)
	return toApply, leafIssuerRef
}

func issuerNamespacedName(
	pandaClusterName, pandaClusterNamespace, name string,
) types.NamespacedName {
	return types.NamespacedName{Name: pandaClusterName + "-" + name, Namespace: pandaClusterNamespace}
}

func (ac *apiCertificates) resources(
	ctx context.Context, k8sClient client.Client, logger logr.Logger,
) ([]resources.Resource, error) {
	if !ac.tlsEnabled {
		return []resources.Resource{}, nil
	}
	nodeSecretRef := ac.externalNodeCertificate
	if nodeSecretRef != nil && nodeSecretRef.Name != "" && nodeSecretRef.Namespace != ac.clusterNamespace {
		if err := copyNodeSecretToLocalNamespace(ctx, nodeSecretRef, ac.clusterNamespace, k8sClient, logger); err != nil {
			return nil, fmt.Errorf("copy node secret for %s cert group: %w", ac.nodeCertificateName().Name, err)
		}
	}

	res := []resources.Resource{}
	res = append(res, ac.rootResources...)
	if ac.nodeCertificate != nil {
		res = append(res, ac.nodeCertificate)
	}
	res = append(res, ac.clientCertificates...)
	return res, nil
}

// Creates copy of secret in Redpanda cluster's namespace
func copyNodeSecretToLocalNamespace(
	ctx context.Context,
	secretRef *corev1.ObjectReference,
	namespace string,
	k8sClient client.Client,
	logger logr.Logger,
) error {
	var secret corev1.Secret
	err := k8sClient.Get(ctx, types.NamespacedName{Name: secretRef.Name, Namespace: secretRef.Namespace}, &secret)
	if err != nil {
		return err
	}

	tlsKey := secret.Data[corev1.TLSPrivateKeyKey]
	tlsCrt := secret.Data[corev1.TLSCertKey]
	caCrt := secret.Data[cmmetav1.TLSCAKey]

	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secret.Name,
			Namespace: namespace,
			Labels:    secret.Labels,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "Secret",
			APIVersion: "v1",
		},
		Type: secret.Type,
		Data: map[string][]byte{
			cmmetav1.TLSCAKey:       caCrt,
			corev1.TLSCertKey:       tlsCrt,
			corev1.TLSPrivateKeyKey: tlsKey,
		},
	}
	_, err = resources.CreateIfNotExists(ctx, k8sClient, caSecret, logger)
	return err
}

func (ac *apiCertificates) nodeCertificateName() *types.NamespacedName {
	if ac.externalNodeCertificate != nil {
		return &types.NamespacedName{
			Name:      ac.externalNodeCertificate.Name,
			Namespace: ac.externalNodeCertificate.Namespace,
		}
	}
	if ac.nodeCertificate != nil {
		name := ac.nodeCertificate.Key()
		return &name
	}
	return nil
}

func (ac *apiCertificates) clientCertificateNames() []types.NamespacedName {
	names := []types.NamespacedName{}
	for _, c := range ac.clientCertificates {
		names = append(names, c.Key())
	}
	return names
}

// Resources returns all resources that need to exist in the cluster to support
// TLS on all redpanda APIs where TLS is enabled
func (cc *ClusterCertificates) Resources(
	ctx context.Context,
) ([]resources.Resource, error) {
	res := []resources.Resource{}
	kafkaResources, err := cc.kafkaAPI.resources(ctx, cc.client, cc.logger)
	if err != nil {
		return nil, fmt.Errorf("retrieving kafkaapi resources %w", err)
	}
	adminResources, err := cc.adminAPI.resources(ctx, cc.client, cc.logger)
	if err != nil {
		return nil, fmt.Errorf("retrieving adminapi resources %w", err)
	}
	pandaProxyResources, err := cc.pandaProxyAPI.resources(ctx, cc.client, cc.logger)
	if err != nil {
		return nil, fmt.Errorf("retrieving pandaproxyapi resources %w", err)
	}
	schemaRegistryResources, err := cc.schemaRegistryAPI.resources(ctx, cc.client, cc.logger)
	if err != nil {
		return nil, fmt.Errorf("retrieving schemaRegistryapi resources %w", err)
	}

	res = append(res, kafkaResources...)
	res = append(res, adminResources...)
	res = append(res, pandaProxyResources...)
	res = append(res, schemaRegistryResources...)
	return res, nil
}

// Volumes returns volumes and mounts that statefulset has to define to have
// access to all TLS certificates redpanda has enabled
func (cc *ClusterCertificates) Volumes() (
	[]corev1.Volume,
	[]corev1.VolumeMount,
) {
	var vols []corev1.Volume
	var mounts []corev1.VolumeMount
	mountPoints := resourcetypes.GetTLSMountPoints()

	vol, mount := secretVolumesForTLS(cc.kafkaAPI.nodeCertificateName(), cc.kafkaAPI.clientCertificates, redpandaCertVolName, redpandaCAVolName, mountPoints.KafkaAPI.NodeCertMountDir, mountPoints.KafkaAPI.ClientCAMountDir)
	vols = append(vols, vol...)
	mounts = append(mounts, mount...)

	vol, mount = secretVolumesForTLS(cc.adminAPI.nodeCertificateName(), cc.adminAPI.clientCertificates, adminAPICertVolName, adminAPICAVolName, mountPoints.AdminAPI.NodeCertMountDir, mountPoints.AdminAPI.ClientCAMountDir)
	vols = append(vols, vol...)
	mounts = append(mounts, mount...)

	vol, mount = secretVolumesForTLS(cc.pandaProxyAPI.nodeCertificateName(), cc.pandaProxyAPI.clientCertificates, pandaProxyCertVolName, pandaProxyCAVolName, mountPoints.PandaProxyAPI.NodeCertMountDir, mountPoints.PandaProxyAPI.ClientCAMountDir)
	vols = append(vols, vol...)
	mounts = append(mounts, mount...)

	vol, mount = secretVolumesForTLS(cc.schemaRegistryAPI.nodeCertificateName(), cc.schemaRegistryAPI.clientCertificates, schemaRegistryCertVolName, schemaRegistryCAVolName, mountPoints.SchemaRegistryAPI.NodeCertMountDir, mountPoints.SchemaRegistryAPI.ClientCAMountDir)
	vols = append(vols, vol...)
	mounts = append(mounts, mount...)

	return vols, mounts
}

func secretVolumesForTLS(
	nodeCertificate *types.NamespacedName,
	clientCertificates []resources.Resource,
	volumeName, caVolumeName, mountDir, caMountDir string,
) ([]corev1.Volume, []corev1.VolumeMount) {
	var vols []corev1.Volume
	var mounts []corev1.VolumeMount
	if nodeCertificate == nil {
		return vols, mounts
	}

	// mount node certificate's private key
	vols = append(vols, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: nodeCertificate.Name,
				Items: []corev1.KeyToPath{
					{
						Key:  corev1.TLSPrivateKeyKey,
						Path: corev1.TLSPrivateKeyKey,
					},
					{
						Key:  corev1.TLSCertKey,
						Path: corev1.TLSCertKey,
					},
				},
			},
		},
	})
	mounts = append(mounts, corev1.VolumeMount{
		Name:      volumeName,
		MountPath: mountDir,
	})

	// if mutual TLS is enabled, mount also client cerificate CA to be able to
	// verify client certificates
	if len(clientCertificates) > 0 {
		vols = append(vols, corev1.Volume{
			Name: caVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: clientCertificates[0].Key().Name,
					Items: []corev1.KeyToPath{
						{
							Key:  cmmetav1.TLSCAKey,
							Path: cmmetav1.TLSCAKey,
						},
					},
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name:      caVolumeName,
			MountPath: caMountDir,
		})
	}

	return vols, mounts
}

// GetTLSConfig returns TLS config for adminAPI that can then be used to connect
// to the admin API of the current cluster
func (cc *ClusterCertificates) GetTLSConfig(
	ctx context.Context, k8sClient client.Reader,
) (*tls.Config, error) {
	nodeCertificateName := cc.adminAPI.nodeCertificateName()
	if nodeCertificateName == nil {
		return nil, errNoTLSError
	}
	tlsConfig := tls.Config{MinVersion: tls.VersionTLS12} // TLS12 is min version allowed by gosec.

	var nodeCertSecret corev1.Secret
	err := k8sClient.Get(ctx, *nodeCertificateName, &nodeCertSecret)
	if err != nil {
		return nil, err
	}

	// Add root CA
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(nodeCertSecret.Data[cmmetav1.TLSCAKey])
	tlsConfig.RootCAs = caCertPool

	if len(cc.adminAPI.clientCertificates) > 0 {
		var clientCertSecret corev1.Secret
		err := k8sClient.Get(ctx, cc.adminAPI.clientCertificateNames()[0], &clientCertSecret)
		if err != nil {
			return nil, err
		}
		cert, err := tls.X509KeyPair(clientCertSecret.Data[corev1.TLSCertKey], clientCertSecret.Data[corev1.TLSPrivateKeyKey])
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return &tlsConfig, nil
}
