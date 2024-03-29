ZONES="$(gcloud compute zones list --format='get(name)' --sort-by='~name')"

for zone in $ZONES:
do
  gcloud compute instances create instance-20240329-154327 \
    --project=rp-byoc-tyler \
    --zone=$zone \
    --machine-type=a2-highgpu-1g \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --maintenance-policy=TERMINATE \
    --provisioning-model=STANDARD \
    --local-ssd-recovery-timeout=1 \
    --service-account=18965308480-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --accelerator=count=1,type=nvidia-tesla-a100 \
    --create-disk="auto-delete=yes,boot=yes,device-name=instance-20240329-154327,image=projects/ubuntu-os-cloud/global/images/ubuntu-2310-mantic-amd64-v20240319,mode=rw,size=100,type=projects/rp-byoc-tyler/zones/$zone/diskTypes/pd-balanced" \
    --local-ssd=interface=NVME \
    --local-ssd=interface=NVME \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ec-src=vm_add-gcloud \
    --reservation-affinity=any
  if [ $? -eq 0 ]; then
      echo OK
      exit 0
  else
      echo FAIL
  fi
done
