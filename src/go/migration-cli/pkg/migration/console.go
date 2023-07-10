package migration

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
	vectorizedv1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/vectorized/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/pointer"
)

func MigrateConsole(console *vectorizedv1alpha1.Console, rp *v1alpha1.Redpanda) {
	if console == nil {
		rpClusterSpec := rp.Spec.ClusterSpec
		if rpClusterSpec == nil {
			return
		}

		if rpClusterSpec.Console == nil {
			rpClusterSpec.Console = &v1alpha1.RedpandaConsole{}
		}
		rpClusterSpec.Console.Enabled = pointer.Bool(false)
		return
	}

	rpClusterSpec := rp.Spec.ClusterSpec
	if rpClusterSpec == nil {
		rpClusterSpec = &v1alpha1.RedpandaClusterSpec{}
	}

	if rpClusterSpec.Console == nil {
		rpClusterSpec.Console = &v1alpha1.RedpandaConsole{}
	}
	rpConsole := rpClusterSpec.Console

	rpConsole.Enabled = pointer.Bool(true)

	rpConsole.ConfigMap = &v1alpha1.ConsoleCreateObj{
		Create: false,
	}

	rpConsole.Deployment = &v1alpha1.ConsoleCreateObj{
		Create: false,
	}

	rpConsole.Secret = &v1alpha1.ConsoleCreateObj{
		Create: false,
	}

	configObj := map[string]interface{}{}

	consoleSpec := console.Spec

	// --- connect ---
	connect := make(map[string]interface{}, 0)
	connect["enabled"] = consoleSpec.Connect.Enabled

	if consoleSpec.Connect.ConnectTimeout != nil {
		connect["connectTimeout"] = consoleSpec.Connect.ConnectTimeout
	}

	if consoleSpec.Connect.ReadTimeout != nil {
		connect["readTimeout"] = consoleSpec.Connect.ReadTimeout
	}

	if consoleSpec.Connect.RequestTimeout != nil {
		connect["requestTimeout"] = consoleSpec.Connect.RequestTimeout
	}

	if len(consoleSpec.Connect.Clusters) > 0 {
		connect["clusters"] = consoleSpec.Connect.Clusters
	}

	// -- finish config ---
	if len(connect) > 0 {
		configObj["connect"] = connect
		rpConsole.Config = &runtime.RawExtension{}
	}

	// -- server ---
	server := make(map[string]interface{}, 0)

	if consoleSpec.Server.HTTPListenPort != 0 {
		server["listenPort"] = consoleSpec.Server.HTTPListenPort
	}

	if consoleSpec.Server.HTTPListenAddress != "" {
		server["listenAddress"] = consoleSpec.Server.HTTPListenAddress
	}

	if consoleSpec.Server.ServerGracefulShutdownTimeout != nil {
		server["gracefulShutdownTimeout"] = consoleSpec.Server.ServerGracefulShutdownTimeout
	}

	if consoleSpec.Server.BasePath != "" {
		server["basePath"] = consoleSpec.Server.BasePath
	}

	// ignore this, manually add if required
	//if consoleSpec.Server.SetBasePathFromXForwardedPrefix != true {
	//	server["setBasePathFromXForwardedPrefix"] = consoleSpec.Server.SetBasePathFromXForwardedPrefix
	//}

	// ignore this, manually add if required
	//if consoleSpec.Server.StripPrefix != true {
	//	server["stripPrefix"] = consoleSpec.Server.StripPrefix
	//}

	if consoleSpec.Server.HTTPServerIdleTimeout != nil {
		server["idleTimeout"] = consoleSpec.Server.HTTPServerIdleTimeout
	}

	// ignore this, manually add if required
	//if consoleSpec.Server.CompressionLevel != 4 {
	//	server["compressionLevel"] = consoleSpec.Server.CompressionLevel
	//}

	// compare to 4 as this is the default
	if consoleSpec.Server.HTTPServerReadTimeout != nil {
		server["readTimeout"] = consoleSpec.Server.HTTPServerReadTimeout
	}

	// compare to 4 as this is the default
	if consoleSpec.Server.HTTPServerWriteTimeout != nil {
		server["writeTimeout"] = consoleSpec.Server.HTTPServerWriteTimeout
	}

	// -- finish server ---
	if len(server) > 0 {
		configObj["server"] = server
		jsonBytes, err := jsoniter.Marshal(configObj)
		if err != nil {
			fmt.Printf("marshalling console config data: %s\n", err)
		}

		rpConsole.Config = &runtime.RawExtension{}
		err = rpConsole.Config.UnmarshalJSON(jsonBytes)
		if err != nil {
			fmt.Printf("unable to unmarshal config data: %s\n", err)
		}
	}
}
