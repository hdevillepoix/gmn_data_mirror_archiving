#!/bin/bash
############################################################
# Submit Acacia -> Banksia archive jobs for one or more GMN years.
# Run on a Setonix login node, inside tmux.
#
# For each year: lists ${ACACIA_REMOTE}:${ACACIA_BUCKET_PREFIX}<year>, skips
# stations already in archive_done.tsv, and submits one SLURM array task per
# remaining station (gmn_banksia_archive_station.sbatch) - one tar per
# year-station, whatever its size - at most MAX_PARALLEL at a time.
#
# A station archived earlier whose file count on Acacia has since changed
# is reported, not re-archived (its tar on Banksia is left as is).
#
# After submitting it stays in the foreground and watches the jobs until
# they finish (gmnWatch: progress, failures with their error line, final
# summary). --no-watch just submits and exits.
#
# Usage: gmn_banksia_archive_submit.sh [--dry-run] [--no-watch] YEAR [YEAR...]
#        gmn_banksia_archive_submit.sh [--dry-run] [--no-watch] --all
############################################################
set -u

GMN_HOME=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export GMN_HOME
source "${GMN_HOME}/gmn_banksia_lib.sh"
gmnLoadConf

usage() {
    echo "Usage: ${0##*/} [--dry-run] [--no-watch] YEAR [YEAR...]"
    echo "       ${0##*/} [--dry-run] [--no-watch] --all"
}

dryRun=FALSE
watch=TRUE
allYears=FALSE
years=()
while [ $# -gt 0 ]
do
    case ${1} in
	--dry-run) dryRun=TRUE ;;
	--no-watch) watch=FALSE ;;
	--all) allYears=TRUE ;;
	-h|--help) usage; exit 0 ;;
	[0-9][0-9][0-9][0-9]) years+=("${1}") ;;
	*) usage; exit 1 ;;
    esac
    shift
done

if [ "${allYears}" = "TRUE" ]
then
    mapfile -t years < <(rclone lsf --dirs-only "${ACACIA_REMOTE}:" \
	| sed -nE "s|^${ACACIA_BUCKET_PREFIX}([0-9]{4})/$|\1|p")
fi
if [ ${#years[@]} -eq 0 ]
then
    usage
    exit 1
fi

exec 9>"${STATE_DIR}/submit.lock"
if ! flock -n 9
then
    echo "ERROR: another submitter holds ${STATE_DIR}/submit.lock"
    exit 1
fi

GMN_LOG="${STATE_DIR}/logs/archive_submit_$(date +%Y%m%d_%H%M%S).log"
if [ "${dryRun}" = "FALSE" ]
then
    gmnSetupPshell || exit 1
    gmnRequireDelegate "${GMN_LOG}" || exit 1
fi

#----------------------------------------------------
### planYear(): writes <outDir>/stations.tsv (station, bytes, nfiles - all
### stations on Acacia) and <outDir>/pending.tsv (same, minus archived ones).
# params: $1 year, $2 outDir
#----------------------------------------------------
planYear() {
    local year=${1}
    local outDir=${2}
    local bucket="${ACACIA_REMOTE}:${ACACIA_BUCKET_PREFIX}${year}"
    local st bytes nfiles firstPath

    gmnLog "Listing ${bucket} ..."
    if ! rclone lsf -R --files-only --format sp --separator $'\t' "${bucket}" > "${outDir}/listing.tsv"
    then
	gmnLog "ERROR: listing ${bucket} failed (see rclone error above)"
	return 1
    fi

    # Listing can work while reading objects is denied (seen as 403 AccessDenied
    # with the wrong rclone remote) - fail here rather than in every job.
    firstPath=$(head -1 "${outDir}/listing.tsv" | cut -f2)
    if [ -n "${firstPath}" ] && ! rclone cat --head 1 "${bucket}/${firstPath}" > /dev/null
    then
	gmnLog "ERROR: can list but not read objects in ${bucket} (tried ${firstPath}) - check the rclone remote/keys (ACACIA_REMOTE=${ACACIA_REMOTE})"
	return 1
    fi

    # per station: bytes, nfiles (files not inside a station dir are ignored)
    awk -F'\t' '
	{ i = index($2, "/"); if (!i) next; st = substr($2, 1, i - 1); b[st] += $1; n[st]++ }
	END { for (st in b) printf "%s\t%.0f\t%d\n", st, b[st], n[st] }
    ' "${outDir}/listing.tsv" | LC_ALL=C sort > "${outDir}/stations.tsv"

    : > "${outDir}/pending.tsv"
    while IFS=$'\t' read -r st bytes nfiles
    do
	if gmnLedgerHas "${ARCHIVE_LEDGER}" "${year}" "${st}"
	then
	    local archivedFiles
	    archivedFiles=$(awk -F'\t' -v y="${year}" -v st="${st}" '$1 == y && $2 == st { print $5 }' "${ARCHIVE_LEDGER}" | tail -1)
	    [ "${archivedFiles}" = "${nfiles}" ] \
		|| gmnLog "WARNING: ${year} ${st} was archived with ${archivedFiles} files but Acacia now has ${nfiles} - not re-archiving"
	else
	    printf '%s\t%s\t%s\n' "${st}" "${bytes}" "${nfiles}" >> "${outDir}/pending.tsv"
	fi
    done < "${outDir}/stations.tsv"
}

GMN_JOB_NAMES=()
watchFile="${STATE_DIR}/lists/archive_watch_$(date +%Y%m%d_%H%M%S).tsv"
: > "${watchFile}"

rc=0
for year in "${years[@]}"
do
    jobName="gmn_arch_${year}"
    gmnLog "=====[ ${year} ]====="
    if gmnJobsQueued "${jobName}"
    then
	gmnLog "SKIP ${year}: ${jobName} jobs are still queued/running - rerun once they finish"
	continue
    fi

    workTmp=$(mktemp -d)
    if ! planYear "${year}" "${workTmp}"
    then
	rm -rf "${workTmp}"
	rc=1
	continue
    fi

    nStations=$(wc -l < "${workTmp}/stations.tsv")
    nFiles=$(wc -l < "${workTmp}/listing.tsv")
    totalBytes=$(awk -F'\t' '{ s += $2 } END { printf "%.0f\n", s }' "${workTmp}/stations.tsv")
    nPending=$(wc -l < "${workTmp}/pending.tsv")
    gmnLog "${year}: ${nStations} stations, ${nFiles} files, $(gmnHuman "${totalBytes}") on Acacia; $(( nStations - nPending )) already archived, ${nPending} to do"

    if [ "${nPending}" -eq 0 ]
    then
	rm -rf "${workTmp}"
	continue
    fi

    pendingBytes=$(awk -F'\t' '{ s += $2 } END { printf "%.0f\n", s }' "${workTmp}/pending.tsv")
    biggest=$(sort -t $'\t' -k2,2nr "${workTmp}/pending.tsv" | head -1)
    peak=$(gmnPeakScratch "${workTmp}/pending.tsv" 2 1)
    gmnLog "${year}: to archive $(gmnHuman "${pendingBytes}"), biggest station ${biggest%%$'\t'*} $(gmnHuman "$(echo "${biggest}" | cut -f2)"), est. peak scratch $(gmnHuman "${peak}") (MAX_PARALLEL=${MAX_PARALLEL}, budget $(gmnHuman "${SCRATCH_BUDGET_BYTES}"))"
    if [ "${peak}" -gt "${SCRATCH_BUDGET_BYTES}" ]
    then
	gmnLog "ERROR: ${year}: peak scratch over budget - lower MAX_PARALLEL or raise SCRATCH_BUDGET_BYTES"
	rm -rf "${workTmp}"
	rc=1
	continue
    fi

    if [ "${dryRun}" = "TRUE" ]
    then
	echo "Would submit (station, bytes, nfiles):"
	column -t "${workTmp}/pending.tsv" | sed 's/^/  /'
	rm -rf "${workTmp}"
	continue
    fi

    listFile="${STATE_DIR}/lists/archive_${year}_$(date +%Y%m%d_%H%M%S).tsv"
    cp "${workTmp}/pending.tsv" "${listFile}"
    rm -rf "${workTmp}"
    while IFS=$'\t' read -r st _
    do
	logf="${STATE_DIR}/logs/archive_${year}_${st}.log"
	gmnRotateLog "${logf}"
	printf '%s\t%s\n' "${year} ${st}" "${logf}" >> "${watchFile}"
    done < "${listFile}"
    gmnSubmitArrays "${jobName}" "${GMN_HOME}/gmn_banksia_archive_station.sbatch" \
	"${SLURM_WALLTIME_ARCHIVE}" "${year}" "${listFile}" || rc=1
done

if [ "${dryRun}" = "FALSE" ] && [ ${#GMN_JOB_NAMES[@]} -gt 0 ]
then
    if [ "${watch}" = "TRUE" ]
    then
	gmnWatch "${watchFile}" || rc=1
    else
	gmnLog "Not watching. Logs: ${STATE_DIR}/logs/archive_<year>_<station>.log"
    fi
fi
gmnLog "Submit log: ${GMN_LOG}"
exit ${rc}
