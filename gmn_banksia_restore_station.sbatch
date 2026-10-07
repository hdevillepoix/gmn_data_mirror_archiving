#!/bin/bash
#SBATCH --partition=copy
#SBATCH --ntasks=1
### Fallback only: the submitter passes partition/account/etc on the sbatch
### command line from config/gmn_banksia.conf, which overrides these.
############################################################
# One SLURM array task = restore one year-station tar Banksia -> Acacia.
# Submitted by gmn_banksia_restore_submit.sh, which sets partition, account,
# walltime, cpus, the array range and exports GMN_HOME/GMN_CONF.
#
#   1. pshell get the tar + .sha256 into scratch (may wait on tape recall),
#      sha256sum -c
#   2a. default: extract, drop the tar, rclone copy --immutable to
#       <dest>/<station>/, rclone check
#   2b. tar-only: rclone copy tar + .sha256 to <dest>/ as they are
#   3. append to restore_done.tsv
# scancel is safe: the TERM trap logs it, the EXIT trap removes scratch.
#
# Args: <year> <list file> <dest> <tarOnly TRUE|FALSE>
#       (list line ${SLURM_ARRAY_TASK_ID}: station bytes)
############################################################
set -u

year=${1}
listFile=${2}
dest=${3}
tarOnly=${4}

source "${GMN_HOME:?GMN_HOME not exported by the submitter}/gmn_banksia_lib.sh"
gmnLoadConf

IFS=$'\t' read -r station archivedBytes \
    < <(sed -n "${SLURM_ARRAY_TASK_ID}p" "${listFile}")

GMN_LOG="${STATE_DIR}/logs/restore_${year}_${station}.log"
gmnLog "=====[ restore ${year} ${station} -> ${dest} (tar-only ${tarOnly}, job ${SLURM_ARRAY_JOB_ID:-}_${SLURM_ARRAY_TASK_ID}) ]====="

if gmnLedgerHas "${RESTORE_LEDGER}" "${year}" "${station}" "${dest}"
then
    gmnLog "DONE: already restored to ${dest} - nothing to do."
    exit 0
fi

fail() {
    gmnLog "FAILED: $*"
    exit 1
}
trap 'gmnLog "FAILED: cancelled (SIGTERM - scancel or walltime)"; exit 143' TERM INT

workDir=$(gmnScratchDir restore "${year}" "${station}") || fail "cannot create scratch dir"
trap 'gmnCleanupWorkDir "${workDir}" $?' EXIT

tarName=$(gmnTarName "${year}" "${station}")
ns="${MF_ROOT}/${year}"

### 1. get from Banksia + verify
gmnSetupPshell || fail "pshell setup"
gmnRequireDelegate "${GMN_LOG}" || fail "delegate"
pshellScript="${workDir}/get.psh"
{
    echo "cd ${ns}"
    echo "get ${tarName}"
    echo "get ${tarName}.sha256"
} > "${pshellScript}"
gmnLog "Fetching ${ns}/${tarName} ($(gmnHuman "${archivedBytes}"))"
(cd "${workDir}" && gmnPshellRun "${pshellScript}" "${GMN_LOG}") || fail "pshell get"
rm -f "${pshellScript}"
[ -f "${workDir}/${tarName}" ] || fail "${tarName} not present after get"
gmnLog "Verifying sha256"
(cd "${workDir}" && sha256sum -c "${tarName}.sha256") 2>&1 | tee -a "${GMN_LOG}"
[ ${PIPESTATUS[0]} -eq 0 ] || fail "sha256 mismatch"

### 2. copy to Acacia
if [ "${tarOnly}" = "TRUE" ]
then
    gmnLog "Uploading ${tarName} + .sha256 to ${dest}"
    gmnRcloneCopyAsJob "${workDir}" "${dest}" "${GMN_LOG}" --immutable \
	--include "${tarName}" --include "${tarName}.sha256" \
	|| fail "rclone copy"
else
    extractDir="${workDir}/extract"
    mkdir -p "${extractDir}"
    gmnLog "Extracting ${tarName}"
    tar -xf "${workDir}/${tarName}" -C "${extractDir}" || fail "tar -x"
    rm -f "${workDir}/${tarName}"
    [ -d "${extractDir}/${station}" ] || fail "tar did not contain ${station}/"
    gmnLog "Uploading to ${dest}/${station}"
    gmnRcloneCopyAsJob "${extractDir}/${station}" "${dest}/${station}" "${GMN_LOG}" --immutable \
	|| fail "rclone copy"
    rclone check "${extractDir}/${station}" "${dest}/${station}" --one-way 2>&1 | tee -a "${GMN_LOG}"
    [ ${PIPESTATUS[0]} -eq 0 ] || fail "rclone check"
fi

### 3. record
gmnLedgerAppend "${RESTORE_LEDGER}" "${year}" "${station}" "${dest}" "${tarOnly}" \
    "$(date +%Y-%m-%dT%H:%M:%S)" "${SLURM_ARRAY_JOB_ID:-}_${SLURM_ARRAY_TASK_ID}"
gmnLog "DONE: ${tarName} -> ${dest}"
exit 0
