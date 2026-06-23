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
#   ./provision_vm.sh                 # lock down the firewall + create the VM
#                                     # (Docker + 2 GB swap baked in via startup)
#   then follow the printed "Next steps" to configure secrets and start Airflow.
#
# Auth to GCP is the VM's attached service account (trade-pipeline@…, scope
# cloud-platform) via the metadata server — no key file is created or uploaded.
set -euo pipefail

# --- Config (override via env) ------------------------------------------------
PROJECT="${GCP_PROJECT:-trade-390514}"
ZONE="${GCP_ZONE:-us-central1-a}"
NETWORK="${GCP_NETWORK:-default}"
IAP_SSH_RULE="${IAP_SSH_RULE:-allow-iap-ssh}"
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

# --- Network: egress via external IP + inbound lockdown -----------------------
# The VM gets an ephemeral EXTERNAL IP (created below — no --no-address) for
# outbound internet: the startup script's `curl get.docker.com` and the daily
# ingest (Binance via the vision mirror, FRED, Yahoo, Coin Metrics, Docker Hub).
# For a single always-on VM an external IP (~$3.6/mo) is ~10x cheaper than Cloud
# NAT (~$32/mo), so we use it instead of NAT. To keep inbound CLOSED despite the
# public IP, lock down the default VPC: allow SSH only from IAP (35.235.240.0/20)
# and delete the world-open SSH/RDP rules. Outbound is allowed by the default
# egress rule and the stateful firewall lets replies back in, so nothing reaches
# the VM unsolicited. All steps idempotent.
if gcloud compute firewall-rules describe "$IAP_SSH_RULE" --project "$PROJECT" >/dev/null 2>&1; then
  echo "Firewall rule '$IAP_SSH_RULE' already exists — skipping."
else
  echo "Creating IAP-only SSH firewall rule '$IAP_SSH_RULE' ..."
  gcloud compute firewall-rules create "$IAP_SSH_RULE" \
    --project "$PROJECT" \
    --network "$NETWORK" \
    --direction INGRESS --action ALLOW --rules tcp:22 \
    --source-ranges 35.235.240.0/20 \
    --description "Allow SSH only from IAP"
fi

# Remove the default VPC's world-open SSH/RDP (keeps default-allow-internal/icmp).
for _rule in default-allow-ssh default-allow-rdp; do
  if gcloud compute firewall-rules describe "$_rule" --project "$PROJECT" >/dev/null 2>&1; then
    echo "Deleting world-open firewall rule '$_rule' ..."
    gcloud compute firewall-rules delete "$_rule" --project "$PROJECT" -q
  fi
done

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
    --metadata startup-script="$STARTUP_SCRIPT"
  # The VM gets an ephemeral external IP (for egress). Inbound stays closed by
  # the firewall lockdown above; SSH goes through IAP.
fi

cat <<NEXT

VM ready. Next steps (run from your machine):

  # 1) SSH in through IAP (inbound is locked down; SSH only via IAP):
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
