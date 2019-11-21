set -ex

/kaniko/executor \
  --dockerfile=tools/ci/Dockerfile \
  --build-arg "BUILD_TYPE=$BUILD_TYPE" \
  --build-arg "COMPILER=$COMPILER" \
  --destination=gcr.io/$PROJECT_ID/builder-$COMPILER-$BUILD_TYPE:$SHORT_SHA \
  --cache=true \
  --cache-ttl=12h
