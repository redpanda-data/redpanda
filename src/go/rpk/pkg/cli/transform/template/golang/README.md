# Redpanda Golang WASM Transform

To get started you first need to have at least go 1.20 installed.

You can get started by modifying the <code>transform.go</code> file
with your logic.

Once you're ready to test out your transform live you need to:

1. Make sure you have a container running via <code>rpk container start</code>
1. Run <code>rpk transform build</code>
1. Create your topics via <code>rpk topic create</code>
1. Run <code>rpk transform deploy</code>
1. Then use <code>rpk topic produce</code> and <code>rpk topic consume</code>
   to see your transformation live!
