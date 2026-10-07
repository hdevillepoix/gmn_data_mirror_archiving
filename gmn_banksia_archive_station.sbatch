#!/bin/bash
#SBATCH --partition=copy
#SBATCH --ntasks=1
### Fallback only: the submitter passes partition/account/etc on the sbatch
### command line from config/gmn_banksia.conf, which overrides these.
############################################################
# One SLURM array task = archive one year-station Acacia -> Banksia, as one
# tar. Submitted by gmn_banksia_archive_submit.sh, which sets partition,
# account, walltime, cpus, the array range and exports GMN_HOME/GMN_CONF.
#
#   1. rclone the station dir from Acacia to scratch, rclone check
#   2. tar it (--remove-files, so scratch peaks at ~1x the station), write
#      a .sha256 sidecar
#   3. pshell put tar + .sha256 to ${MF_ROOT}/<year>/
#   4. verify remote size, append to archive_done.tsv
# Acacia is never modified. scancel is safe: the TERM trap logs it and the
# EXIT trap removes the scratch dir; nothing is recorded, so a rerun redoes
# the station (overwriting any half-uploaded tar).
#
# Args: <year> <list file> (line ${SLURM_ARRAY_TASK_ID}: station bytes nfiles)
############################################################
set -u

year=${1}
listFile=${2}

source "${GMN_HOME:?GMN_HOME not exported by the submitter}/gmn_banksia_lib.sh"
gmnLoadConf

IFS=$'\t' read -r station listedBytes listedFiles \
    < <(sed -n "${SLURM_ARRAY_TASK_ID}p" "${listFile}")

GMN_LOG="${STATE_DIR}/logs/archive_${year}_${station}.log"
gmnLog "=====[ archive ${year} ${station} (job ${SLURM_ARRAY_JOB_ID:-}_${SLURM_ARRAY_TASK_ID}) ]====="

if gmnLedgerHas "${ARCHIVE_LEDGER}" "${year}" "${station}"
then
    gmnLog "DONE: already archived - nothing to do."
    exit 0
fi

fail() {
    gmnLog "FAILED: $*"
    exit 1
}
trap 'gmnLog "FAILED: cancelled (SIGTERM - scancel or walltime)"; exit 143' TERM INT

workDir=$(gmnScratchDir archive "${year}" "${station}") || fail "cannot create scratch dir"
trap 'gmnCleanupWorkDir "${workDir}" $?' EXIT

src="${ACACIA_REMOTE}:${ACACIA_BUCKET_PREFIX}${year}/${station}"
stageDir="${workDir}/stage/${station}"
tarName=$(gmnTarName "${year}" "${station}")
tarPath="${workDir}/${tarName}"
ns="${MF_ROOT}/${year}"

### 1. download + check
gmnLog "Downloading ${listedFiles} files ($(gmnHuman "${listedBytes}")) from ${src}"
gmnRcloneCopyAsJob "${src}" "${stageDir}" "${GMN_LOG}" || fail "rclone copy"
rclone check "${src}" "${stageDir}" --one-way 2>&1 | tee -a "${GMN_LOG}"
[ ${PIPESTATUS[0]} -eq 0 ] || fail "rclone check"
nLocal=$(find "${stageDir}" -type f | wc -l)
[ "${nLocal}" -ge "${listedFiles}" ] || fail "expected at least ${listedFiles} files, got ${nLocal}"

### 2. tar (contents are already .tar.bz2 - no compression)
gmnLog "Creating ${tarName} (${nLocal} files)"
tar -cf "${tarPath}" -C "${workDir}/stage" --remove-files "${station}" || fail "tar"
tarBytes=$(stat -c %s "${tarPath}")
(cd "${workDir}" && sha256sum "${tarName}" > "${tarName}.sha256") || fail "sha256sum"
nInTar=$(tar -tf "${tarPath}" | grep -vc '/$')
[ "${nInTar}" -eq "${nLocal}" ] || fail "tar holds ${nInTar} files, downloaded ${nLocal}"
sha=$(cut -d' ' -f1 "${tarPath}.sha256")
gmnLog "${tarName}: $(gmnHuman "${tarBytes}"), sha256 ${sha}"

### 3. put to Banksia
gmnSetupPshell || fail "pshell setup"
gmnRequireDelegate "${GMN_LOG}" || fail "delegate"
gmnEnsureNamespace "${ns}" || fail "namespace ${ns}"
pshellScript="${workDir}/put.psh"
{
    echo "cd ${ns}"
    echo "put ${tarPath}"
    echo "put ${tarPath}.sha256"
} > "${pshellScript}"
gmnLog "Uploading ${tarName} to ${ns}"
gmnPshellRun "${pshellScript}" "${GMN_LOG}" || fail "pshell put"

### 4. verify + record
remoteBytes=$(gmnRemoteAssetSize "${ns}/${tarName}") || fail "cannot read remote size of ${ns}/${tarName}"
[ "${remoteBytes}" -eq "${tarBytes}" ] || fail "remote size ${remoteBytes} != local ${tarBytes}"

gmnLedgerAppend "${ARCHIVE_LEDGER}" "${year}" "${station}" "${tarBytes}" "${sha}" \
    "${nLocal}" "$(date +%Y-%m-%dT%H:%M:%S)" "${SLURM_ARRAY_JOB_ID:-}_${SLURM_ARRAY_TASK_ID}"
gmnLog "DONE: ${ns}/${tarName} ($(gmnHuman "${tarBytes}"))"
exit 0
