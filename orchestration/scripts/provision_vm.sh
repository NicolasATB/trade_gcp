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
#   ./provision_vm.sh                 # create Cloud NAT + the VM (Docker + 2 GB
#                                     # swap baked in via the startup script)
#   then follow the printed "Next steps" to configure secrets and start Airflow.
#
# Auth to GCP is the VM's attached service account (trade-pipeline@…, scope
# cloud-platform) via the metadata server — no key file is created or uploaded.
set -euo pipefail

# --- Config (override via env) ------------------------------------------------
PROJECT="${GCP_PROJECT:-trade-390514}"
ZONE="${GCP_ZONE:-us-central1-a}"
REGION="${GCP_REGION:-us-central1}"
NETWORK="${GCP_NETWORK:-default}"
ROUTER_NAME="${ROUTER_NAME:-trade-nat-router}"
NAT_NAME="${NAT_NAME:-trade-nat}"
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

# --- Cloud NAT (egress for the --no-address VM) -------------------------------
# The VM is created with --no-address, so it has no public IP and zero internet
# egress on its own. SSH still works through IAP (Google's internal network),
# but outbound traffic — the startup script's `curl get.docker.com` and the
# daily ingest (Binance, FRED, Yahoo, Coin Metrics, bitcoin-data.com, Docker
# Hub) — needs Cloud NAT. Cloud NAT is preferred over an external IP because the
# `default` VPC has `default-allow-ssh 0.0.0.0/0`, so a public IP would expose
# SSH to the internet. Both steps are idempotent.
if gcloud compute routers describe "$ROUTER_NAME" --region "$REGION" --project "$PROJECT" >/dev/null 2>&1; then
  echo "Cloud Router '$ROUTER_NAME' already exists in $REGION — skipping."
else
  echo "Creating Cloud Router '$ROUTER_NAME' in $REGION ..."
  gcloud compute routers create "$ROUTER_NAME" \
    --project "$PROJECT" \
    --region "$REGION" \
    --network "$NETWORK"
fi

if gcloud compute routers nats describe "$NAT_NAME" --router "$ROUTER_NAME" --region "$REGION" --project "$PROJECT" >/dev/null 2>&1; then
  echo "Cloud NAT '$NAT_NAME' already exists on '$ROUTER_NAME' — skipping."
else
  echo "Creating Cloud NAT '$NAT_NAME' on '$ROUTER_NAME' ..."
  gcloud compute routers nats create "$NAT_NAME" \
    --project "$PROJECT" \
    --router "$ROUTER_NAME" \
    --region "$REGION" \
    --auto-allocate-nat-external-ips \
    --nat-all-subnet-ip-ranges
fi

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

  # 3) Secrets (never committed): fill AIRFLOW_UID, the admin password, a Fernet
  #    key and FRED_API_KEY. No GCP key file — the VM uses its attached SA (ADC).
  cp .env.example .env && nano .env

  # 4) Build and start Airflow (on the VM):
  docker compose build
  docker compose up airflow-init             # one-shot: migrate + create admin
  docker compose up -d                       # scheduler + webserver

  # 5) Reach the UI from your machine via an IAP port-forward, then open
  #    http://localhost:8080 :
  gcloud compute ssh $VM_NAME --zone $ZONE --project $PROJECT \\
      --tunnel-through-iap -- -L 8080:localhost:8080

NEXT
