from ubuntu:noble

run apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
  wget default-jre dotnet-sdk-8.0

run dotnet tool install --global P

env PATH="$PATH:/root/.dotnet/tools"
