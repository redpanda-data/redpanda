ZONES="$(gcloud compute zones list --format='get(name)' --sort-by='~name')"


# GPUs can be hard to get, so try a bunch of zones
for zone in $ZONES:
do
  gcloud compute instances create redpanda-ai \
    --zone=$zone \
    --machine-type=a2-highgpu-1g
  if [ $? -eq 0 ]; then
      echo OK
      exit 0
  else
      echo FAIL
  fi
done
