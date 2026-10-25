#!/bin/bash
############################################################
# Install/update the GMN Acacia <-> Banksia scripts on Setonix.
# Self-contained: everything needed (incl. lib/) comes from this repo.
# Existing state/ (plans, ledgers, logs) on the remote is never touched.
# config/gmn_banksia.conf IS overwritten - the repo copy is authoritative.
#
# Usage: ./install_gmn_banksia.sh [user@host:/remote/path]
############################################################
set -eu

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

remote=${1:-hdevillepoix@setonix.pawsey.org.au:/software/projects/pawsey0851/hdevillepoix/gmn_banksia}
remoteHost=${remote%%:*}
remotePath=${remote#*:}

files=(
    gmn_banksia_lib.sh
    gmn_banksia_archive_submit.sh
    gmn_banksia_archive_station.sbatch
    gmn_banksia_restore_submit.sh
    gmn_banksia_restore_station.sbatch
    gmn_banksia_submit_next.sbatch
    README.md
)

echo "Installing to ${remote}"
ssh "${remoteHost}" "mkdir -p '${remotePath}/lib' '${remotePath}/config' \
    '${remotePath}/state/lists' '${remotePath}/state/logs'"

paths=()
for f in "${files[@]}"; do paths+=("${HERE}/${f}"); done
rsync -av "${paths[@]}" "${remote}/"
rsync -av "${HERE}/lib/mediaflux_lib.sh" "${remote}/lib/"
rsync -av "${HERE}/config/gmn_banksia.conf" "${remote}/config/"

ssh "${remoteHost}" "chmod +x '${remotePath}'/*.sh '${remotePath}'/*.sbatch"

cat <<EOF

Done. On Setonix:
  cd ${remotePath}
  ./gmn_banksia_archive_submit.sh --dry-run 2018
  ./gmn_banksia_archive_submit.sh 2018
EOF
