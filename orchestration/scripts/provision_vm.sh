#!/usr/bin/env bash
#
# Bootstrap the e2-micro orchestration VM and prepare it for the Airflow stack
# (T-11). This is the gcloud bootstrap path; T-14 codifies the same VM in
# Terraform. Re-running is safe: it skips the VM if it already exists.
#
# Prerequisites: gcloud authenticated (`gcloud auth login`) and the project's
# APIs enabled (Compute Engine, IAP for tunneling). Run from Git Bash / Linux.
#
# Usage:
#   ./provision_vm.sh                 # create the VM (Docker + 2 GB swap baked
#                                     # in via the startup script)
#   then follow the printed "Next steps" to upload secrets and start Airflow.
set -euo pipefail

# --- Config (override via env) ------------------------------------------------
PROJECT="${GCP_PROJECT:-trade-390514}"
ZONE="${GCP_ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-trade-airflow}"
MACHINE_TYPE="${MACHINE_TYPE:-e2-micro}"
BOOT_DISK_SIZE="${BOOT_DISK_SIZE:-30GB}"
SA_EMAIL="${SA_EMAIL:-trade-pipeline@trade-390514.iam.gserviceaccount.com}"
IMAGE_FAMILY="${IMAGE_FAMILY:-debian-12}"
IMAGE_PROJECT="${IMAGE_PROJECT:-debian-cloud}"

# Startup script: install Docker + compose plugin and add 2 GB of swap so the
# Airflow stack fits in 1 GB of RAM. Runs as root on first boot.
read -r -d '' STARTUP_SCRIPT <<'EOS' || true
#!/usr/bin/env bash
set -euo pipefail
# 2 GB swap (idempotent)
if ! swapon --show | grep -q /swapfile; then
  fallocate -l 2G /swapfile || dd if=/dev/zero of=/swapfile bs=1M count=2048
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
# Docker Engine + compose plugin (official convenience script)
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sh
  usermod -aG docker "$(getent passwd 1000 | cut -d: -f1)" || true
fi
EOS

# --- Create the VM ------------------------------------------------------------
if gcloud compute instances describe "$VM_NAME" --zone "$ZONE" --project "$PROJECT" >/dev/null 2>&1; then
  echo "VM '$VM_NAME' already exists in $ZONE — skipping creation."
else
  echo "Creating e2-micro VM '$VM_NAME' in $ZONE ..."
  gcloud compute instances create "$VM_NAME" \
    --project "$PROJECT" \
    --zone "$ZONE" \
    --machine-type "$MACHINE_TYPE" \
    --image-family "$IMAGE_FAMILY" \
    --image-project "$IMAGE_PROJECT" \
    --boot-disk-size "$BOOT_DISK_SIZE" \
    --service-account "$SA_EMAIL" \
    --scopes cloud-platform \
    --no-address \
    --metadata startup-script="$STARTUP_SCRIPT"
  # --no-address keeps the VM off the public internet; SSH goes through IAP.
fi

cat <<NEXT

VM ready. Next steps (run from your machine):

  # 1) SSH in through IAP (no public IP):
  gcloud compute ssh $VM_NAME --zone $ZONE --project $PROJECT --tunnel-through-iap

  # On the VM:
  # 2) Clone the repo and enter the orchestration dir:
  git clone <REPO_URL> trade_gcp && cd trade_gcp/orchestration

  # 3) Secrets (never committed):
  cp .env.example .env && nano .env          # fill in real values
  mkdir -p keys                              # then upload the SA key (step 4)

  # 4) From your machine, copy the service-account key to the VM:
  gcloud compute scp <PATH_TO_SA_KEY>.json \\
      $VM_NAME:~/trade_gcp/orchestration/keys/sa.json \\
      --zone $ZONE --project $PROJECT --tunnel-through-iap

  # 5) Build and start Airflow (on the VM):
  docker compose build
  docker compose up airflow-init             # one-shot: migrate + create admin
  docker compose up -d                       # scheduler + webserver

  # 6) Reach the UI from your machine via an IAP port-forward, then open
  #    http://localhost:8080 :
  gcloud compute ssh $VM_NAME --zone $ZONE --project $PROJECT \\
      --tunnel-through-iap -- -L 8080:localhost:8080

NEXT
