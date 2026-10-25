#!/bin/bash
############################################################
# Submit Banksia -> Acacia restore jobs for one GMN year (optionally only
# some stations). Run on a Setonix login node.
#
# The stations to restore come from archive_done.tsv (what was archived,
# with sizes for the scratch estimate). One SLURM array task per
# year-station tar (gmn_banksia_restore_station.sbatch), at most
# MAX_PARALLEL at a time. Stations already in restore_done.tsv for the same
# destination are skipped.
#
# Default destination is the original bucket,
# ${ACACIA_REMOTE}:${ACACIA_BUCKET_PREFIX}<year>, files landing back in
# <station>/. Files are uploaded with --immutable: an existing file with
# different content is never overwritten (the task fails instead).
#
# --dest REMOTE:BUCKET[/PREFIX]  restore under somewhere else instead
# --tar-only                     copy the tars + .sha256 sidecars to
#                                <dest>/ as they are, without extracting
#
# After submitting it stays in the foreground and watches the jobs until
# they finish - run it in tmux. --no-watch just submits and exits.
#
# Usage: gmn_banksia_restore_submit.sh [--dry-run] [--no-watch] [--dest REMOTE:BUCKET] [--tar-only] YEAR [STATION...]
############################################################
set -u

GMN_HOME=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export GMN_HOME
source "${GMN_HOME}/gmn_banksia_lib.sh"
gmnLoadConf

usage() {
    echo "Usage: ${0##*/} [--dry-run] [--no-watch] [--dest REMOTE:BUCKET[/PREFIX]] [--tar-only] YEAR [STATION...]"
}

dryRun=FALSE
watch=TRUE
tarOnly=FALSE
dest=""
year=""
stations=()
while [ $# -gt 0 ]
do
    case ${1} in
	--dry-run) dryRun=TRUE ;;
	--no-watch) watch=FALSE ;;
	--tar-only) tarOnly=TRUE ;;
	--dest) shift; dest=${1:-} ;;
	-h|--help) usage; exit 0 ;;
	-*) usage; exit 1 ;;
	*)
	    if [ -z "${year}" ]
	    then
		year=${1}
	    else
		stations+=("${1}")
	    fi
	    ;;
    esac
    shift
done
if ! [[ "${year}" =~ ^[0-9]{4}$ ]]
then
    usage
    exit 1
fi
dest=${dest:-${ACACIA_REMOTE}:${ACACIA_BUCKET_PREFIX}${year}}
dest=${dest%/}

exec 9>"${STATE_DIR}/submit.lock"
if ! flock -n 9
then
    echo "ERROR: another submitter holds ${STATE_DIR}/submit.lock"
    exit 1
fi

jobName="gmn_rest_${year}"
if gmnJobsQueued "${jobName}"
then
    echo "ERROR: ${jobName} jobs are still queued/running - rerun once they finish"
    exit 1
fi

GMN_LOG="${STATE_DIR}/logs/restore_submit_$(date +%Y%m%d_%H%M%S).log"
gmnLog "Restore ${year} ${stations[*]:-(all stations)} -> ${dest} (tar-only: ${tarOnly})"

# station<TAB>bytes, archived and not yet restored to ${dest}
pendingTmp=$(mktemp)
trap 'rm -f "${pendingTmp}"' EXIT
awk -F'\t' -v y="${year}" -v sts="${stations[*]:-}" '
    BEGIN { n = split(sts, a, " "); for (i = 1; i <= n; i++) want[a[i]] = 1 }
    $1 == y && (n == 0 || ($2 in want)) { print $2 "\t" $3 }
' "${ARCHIVE_LEDGER}" | sort -u \
    | while IFS=$'\t' read -r st bytes
    do
	gmnLedgerHas "${RESTORE_LEDGER}" "${year}" "${st}" "${dest}" \
	    || printf '%s\t%s\n' "${st}" "${bytes}"
    done > "${pendingTmp}"

for st in "${stations[@]}"
do
    awk -F'\t' -v y="${year}" -v st="${st}" '$1 == y && $2 == st { f = 1 } END { exit !f }' "${ARCHIVE_LEDGER}" \
	|| gmnLog "WARNING: ${year} ${st} is not recorded as archived - skipping"
done

nPending=$(wc -l < "${pendingTmp}")
if [ "${nPending}" -eq 0 ]
then
    gmnLog "Nothing to restore."
    exit 0
fi

factor=2
[ "${tarOnly}" = "TRUE" ] && factor=1
pendingBytes=$(awk -F'\t' '{ s += $2 } END { printf "%.0f\n", s }' "${pendingTmp}")
peak=$(gmnPeakScratch "${pendingTmp}" 2 "${factor}")
gmnLog "${nPending} stations, $(gmnHuman "${pendingBytes}"), est. peak scratch $(gmnHuman "${peak}") (MAX_PARALLEL=${MAX_PARALLEL}, budget $(gmnHuman "${SCRATCH_BUDGET_BYTES}"))"
if [ "${peak}" -gt "${SCRATCH_BUDGET_BYTES}" ]
then
    gmnLog "ERROR: peak scratch over budget - lower MAX_PARALLEL or raise SCRATCH_BUDGET_BYTES"
    exit 1
fi

if [ "${dryRun}" = "TRUE" ]
then
    echo "Would submit (station, bytes):"
    column -t "${pendingTmp}" | sed 's/^/  /'
    exit 0
fi

gmnSetupPshell || exit 1
gmnRequireDelegate "${GMN_LOG}" || exit 1

listFile="${STATE_DIR}/lists/restore_${year}_$(date +%Y%m%d_%H%M%S).tsv"
cp "${pendingTmp}" "${listFile}"
watchFile="${listFile%.tsv}.watch"
: > "${watchFile}"
while IFS=$'\t' read -r st _
do
    logf="${STATE_DIR}/logs/restore_${year}_${st}.log"
    gmnRotateLog "${logf}"
    printf '%s\t%s\n' "${year} ${st}" "${logf}" >> "${watchFile}"
done < "${listFile}"

GMN_JOB_NAMES=()
gmnSubmitArrays "${jobName}" "${GMN_HOME}/gmn_banksia_restore_station.sbatch" \
    "${SLURM_WALLTIME_RESTORE}" "${year}" "${listFile}" "${dest}" "${tarOnly}"
rc=$?
if [ ${#GMN_JOB_NAMES[@]} -gt 0 ]
then
    if [ "${watch}" = "TRUE" ]
    then
	gmnWatch "${watchFile}" || rc=1
    else
	gmnLog "Not watching. Logs: ${STATE_DIR}/logs/restore_${year}_<station>.log"
    fi
fi
gmnLog "Submit log: ${GMN_LOG}"
exit ${rc}
