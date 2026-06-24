#!/usr/bin/env bash
set -euo pipefail

WORKSPACE="${V5_WORKSPACE:-/home/ubuntu/clawd/v5-prod}"
EXPORT_DIR="${V5_LIVE_FOLLOWUP_BUNDLE_DIR:-/var/lib/v5/exports/bundles}"
SOURCE_USER="${V5_BUNDLE_SOURCE_USER:-ubuntu}"
DASHBOARD_USER="${V5_DASHBOARD_BUNDLE_USER:-ubuntu}"

apply_dashboard_acl() {
  local target_path="$1"

  if [[ -z "${DASHBOARD_USER}" ]]; then
    echo "WARN_BUNDLE_ACL_SET_SKIPPED reason=dashboard_user_empty target=${target_path}"
    return
  fi
  if ! id "${DASHBOARD_USER}" >/dev/null 2>&1; then
    echo "WARN_BUNDLE_ACL_SET_SKIPPED reason=dashboard_user_missing user=${DASHBOARD_USER} target=${target_path}"
    return
  fi
  if ! command -v setfacl >/dev/null 2>&1; then
    echo "WARN_BUNDLE_ACL_SET_SKIPPED reason=setfacl_unavailable user=${DASHBOARD_USER} target=${target_path}"
    return
  fi

  setfacl -m "u:${DASHBOARD_USER}:rx" "${EXPORT_DIR}"
  setfacl -d -m "u:${DASHBOARD_USER}:rX" "${EXPORT_DIR}"
  setfacl -m "u:${DASHBOARD_USER}:r" "${target_path}"
}

install -d -m 0750 -o root -g v5readonly "${EXPORT_DIR}"

sudo -u "${SOURCE_USER}" bash "${WORKSPACE}/scripts/generate_v5_bundle_remote.sh" "${WORKSPACE}"

latest="$(ls -1t /tmp/v5_live_followup_bundle_*.tar.gz | head -1)"
target="${EXPORT_DIR}/$(basename "${latest}")"
install -m 0640 -o root -g v5readonly "${latest}" "${target}"
apply_dashboard_acl "${target}"

if [[ -f "${latest}.sha256" ]]; then
  sha_target="${EXPORT_DIR}/$(basename "${latest}.sha256")"
  install -m 0640 -o root -g v5readonly "${latest}.sha256" "${sha_target}"
  apply_dashboard_acl "${sha_target}"
fi

python3 "${WORKSPACE}/scripts/prune_v5_bundles.py" "${EXPORT_DIR}" \
  --keep-count 1000 \
  --max-age-days 7
