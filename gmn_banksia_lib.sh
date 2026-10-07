### Shared functions for the GMN Acacia <-> Banksia (Mediaflux) scripts.
###
### Patterns adapted from /opt/server_tools/mediaflux_ingest (pshell setup
### from pshell_ingest_session.sh, srun'd rclone from acacia_stage_lib.sh's
### rcloneCopyAsJob(), pshell retry loop + flock'd namespace creation from
### pshell_ingest_session_lib.sh's mediafluxIngestSession()).
###
### Requires GMN_HOME to be set to the repo/deploy root before sourcing.

source "${GMN_HOME:?GMN_HOME not set}/lib/mediaflux_lib.sh"

#----------------------------------------------------
### gmnLoadConf(): sources ${GMN_CONF} (default config/gmn_banksia.conf)
### and makes sure the durable state layout exists.
#----------------------------------------------------
gmnLoadConf() {
    GMN_CONF=${GMN_CONF:-${GMN_HOME}/config/gmn_banksia.conf}
    source "${GMN_CONF}"
    isDebug=${isDebug:-FALSE}
    mkdir -p "${STATE_DIR}/lists" "${STATE_DIR}/logs"
    ARCHIVE_LEDGER="${STATE_DIR}/archive_done.tsv"
    RESTORE_LEDGER="${STATE_DIR}/restore_done.tsv"
    touch "${ARCHIVE_LEDGER}" "${RESTORE_LEDGER}"
}

#----------------------------------------------------
### gmnLog(): timestamped message to stdout and ${GMN_LOG} (if set).
#----------------------------------------------------
gmnLog() {
    local msg="$( date +"%Y-%m-%d %H:%M:%S" ) $*"
    if [ -n "${GMN_LOG:-}" ]
    then
	echo "${msg}" | tee -a "${GMN_LOG}"
    else
	echo "${msg}"
    fi
}

#----------------------------------------------------
### gmnSetupPshell(): sets PSHELL_INVOKE (plain, used for queries whose
### output gets parsed) and PSHELL_SCRIPT_INVOKE (for put/get scripts,
### optionally verbose). On Setonix pshell comes from the module system.
#----------------------------------------------------
gmnSetupPshell() {
    if command -v module >/dev/null 2>&1
    then
	module load "${PSHELL_MODULE}"
    fi
    if ! command -v pshell >/dev/null 2>&1
    then
	gmnLog "ERROR: pshell not found (module ${PSHELL_MODULE})"
	return 1
    fi
    PSHELL_INVOKE="pshell"
    PSHELL_SCRIPT_INVOKE="pshell"
    [ "${PSHELL_VERBOSE:-FALSE}" = "TRUE" ] && PSHELL_SCRIPT_INVOKE="pshell -v 2"
    return 0
}

#----------------------------------------------------
### gmnRequireDelegate(): non-interactive replacement for
### waitForDatastoreDelegate(). OK (0) or expiring soon (1) passes; a
### network problem (3) is retried every 10 min for up to an hour; an
### expired/broken delegate fails straight away (nobody is there to log in).
# params: $1 logFile
#----------------------------------------------------
gmnRequireDelegate() {
    local logFile=${1}
    local status try
    for try in 1 2 3 4 5 6 7
    do
	checkDatastoreDelegate "${logFile}" status
	case ${status} in
	    0|1) return 0 ;;
	    3) gmnLog "Mediaflux unreachable (try ${try}) - retrying in 10 min"; sleep 600 ;;
	    *) gmnLog "ERROR: Mediaflux delegate invalid (status ${status}) - re-delegate and resubmit"; return 1 ;;
	esac
    done
    return 1
}

#----------------------------------------------------
### gmnPshellRun(): runs a pshell script (-i) with up to 3 tries,
### re-checking the delegate between tries.
# params: $1 pshell script file, $2 logFile
#----------------------------------------------------
gmnPshellRun() {
    local pshellScript=${1}
    local logFile=${2}
    local try rc out
    for try in 1 2 3
    do
	gmnLog "pshell try #${try}, script:"
	cat "${pshellScript}" | tee -a "${logFile}"
	out=$(${PSHELL_SCRIPT_INVOKE} -i "${pshellScript}" 2>&1)
	rc=$?
	echo "${out}" | tee -a "${logFile}"
	[ ${rc} -eq 0 ] && return 0
	gmnLog "pshell exited ${rc}"
	[ ${try} -lt 3 ] || break
	gmnRequireDelegate "${logFile}" || return 1
	sleep 60
    done
    gmnLog "ERROR: pshell failed after 3 tries"
    return 1
}

#----------------------------------------------------
### gmnEnsureNamespace(): creates ${MF_ROOT} and every level below it down
### to $1. Serialized via flock across concurrent array tasks, since
### check-then-mkdir isn't atomic on the Mediaflux side.
# params: $1 namespace (must be ${MF_ROOT} or below it)
#----------------------------------------------------
gmnEnsureNamespace() {
    local ns=${1}
    local lockFd rc=0 path part parts=()
    [ "${ns}" != "${MF_ROOT}" ] && IFS=/ read -ra parts <<< "${ns#${MF_ROOT}/}"
    exec {lockFd}>"${STATE_DIR}/mediaflux_ns.lock"
    flock -x "${lockFd}"
    path=${MF_ROOT}
    createPawseyDatastoreDirIfNotExist "${path}" || rc=$?
    for part in "${parts[@]}"
    do
	[ ${rc} -eq 0 ] || break
	path="${path}/${part}"
	createPawseyDatastoreDirIfNotExist "${path}" || rc=$?
    done
    flock -u "${lockFd}"
    exec {lockFd}>&-
    [ ${rc} -eq 0 ] || gmnLog "ERROR: could not create namespace ${path} (rc ${rc})"
    return ${rc}
}

#----------------------------------------------------
### gmnRemoteAssetSize(): echoes the content size in bytes of a Mediaflux
### asset, or returns 1.
### TODO confirm on Setonix: exact pshell output format of this query - the
### parse just takes the first integer following "size".
# params: $1 full asset path
#----------------------------------------------------
gmnRemoteAssetSize() {
    local assetPath=${1}
    local out size
    out=$(${PSHELL_INVOKE} "asset.get :id path=${assetPath} :xpath -ename size content/size" 2>&1) || return 1
    size=$(echo "${out}" | sed -nE 's/.*size[^0-9]*([0-9]+).*/\1/p' | head -1)
    [ -n "${size}" ] || return 1
    echo "${size}"
}

#----------------------------------------------------
### gmnRcloneCopyAsJob(): `rclone copy` as a SLURM job step (a step of the
### surrounding array task's allocation).
# params: $1 src, $2 dest, $3 logFile, $4... extra rclone args
# Returns rclone's exit code.
#----------------------------------------------------
gmnRcloneCopyAsJob() {
    local src=${1}
    local dest=${2}
    local logFile=${3}
    shift 3
    srun --ntasks=1 --cpus-per-task="${RCLONE_CPUS}" \
	rclone copy "${src}" "${dest}" \
	--transfers "${RCLONE_TRANSFERS}" --checkers "${RCLONE_CHECKERS}" \
	--local-no-set-modtime \
	--stats 1m --stats-one-line --stats-log-level NOTICE \
	--checksum --retries 3 --low-level-retries 10 "$@" 2>&1 | tee -a "${logFile}"
    return ${PIPESTATUS[0]}
}

#----------------------------------------------------
### Ledgers: tab-separated, one line per completed year-station, appended under
### flock. gmnLedgerHas matches the given values against the leading columns.
# gmnLedgerHas    params: $1 ledger file, $2... leading column values
# gmnLedgerAppend params: $1 ledger file, $2... column values
#----------------------------------------------------
gmnLedgerHas() {
    local ledger=${1}
    shift
    local key
    key=$(printf '%s\t' "$@")
    awk -v k="${key}" 'index($0 "\t", k) == 1 { found=1; exit } END { exit !found }' "${ledger}"
}

gmnLedgerAppend() {
    local ledger=${1}
    shift
    local lockFd
    exec {lockFd}>"${ledger}.lock"
    flock -x "${lockFd}"
    (IFS=$'\t'; echo "$*") >> "${ledger}"
    flock -u "${lockFd}"
    exec {lockFd}>&-
}

#----------------------------------------------------
### gmnTarName(): Banksia object name for one year-station.
# params: $1 year, $2 station
#----------------------------------------------------
gmnTarName() {
    echo "${ACACIA_BUCKET_PREFIX}${1}_${2}.tar"
}

#----------------------------------------------------
### gmnScratchDir(): creates and echoes a fresh per-station scratch work dir.
# params: $1 mode (archive|restore), $2 year, $3 station
#----------------------------------------------------
gmnScratchDir() {
    local scratchRoot=${!SCRATCH_ROOT_ENV:?scratch root env var ${SCRATCH_ROOT_ENV} not set}
    local dir="${scratchRoot}/gmn_banksia/${1}/${2}/${3}"
    rm -rf "${dir}"
    mkdir -p "${dir}"
    echo "${dir}"
}

#----------------------------------------------------
### gmnCleanupWorkDir(): removes a station's scratch dir, unless
### KEEP_SCRATCH_ON_FAILURE=TRUE and the job is failing (debugging hatch).
# params: $1 dir, $2 exit status of the job
#----------------------------------------------------
gmnCleanupWorkDir() {
    local dir=${1}
    local status=${2}
    if [ "${status}" -ne 0 ] && [ "${KEEP_SCRATCH_ON_FAILURE:-FALSE}" = "TRUE" ]
    then
	echo "KEEP_SCRATCH_ON_FAILURE=TRUE - leaving ${dir} in place."
	return
    fi
    rm -rf "${dir}"
}

#----------------------------------------------------
### gmnPeakScratch(): echoes the estimated peak scratch in bytes for a work
### list: the MAX_PARALLEL biggest stations running together, times a factor.
# params: $1 list file, $2 column holding station bytes, $3 factor
#----------------------------------------------------
gmnPeakScratch() {
    cut -f "${2}" "${1}" | sort -nr | head -n "${MAX_PARALLEL}" \
	| awk -v f="${3}" '{ s += $1 } END { printf "%.0f\n", s * f }'
}

gmnHuman() {
    numfmt --to=iec --suffix=B "${1}"
}

#----------------------------------------------------
### gmnSubmitArrays(): submits one SLURM array task per line of a work list,
### at most MAX_PARALLEL running. Lists longer than SLURM_ARRAY_MAX are cut
### into several arrays chained with afterany, so the limit holds overall.
### Each task gets: <year> <batch list file> <extra args...>.
### Appends the submitted job ids to the global GMN_JOB_IDS array (so don't
### call this in a pipeline/subshell).
# params: $1 job name, $2 sbatch script, $3 walltime, $4 year, $5 list file,
#         $6... extra args for the sbatch script
#----------------------------------------------------
gmnSubmitArrays() {
    local jobName=${1}
    local sbatchScript=${2}
    local walltime=${3}
    local year=${4}
    local listFile=${5}
    shift 5
    local nLines batchFile batchN dependency="" jobId start=1 batchIdx=1
    nLines=$(wc -l < "${listFile}")
    while [ ${start} -le ${nLines} ]
    do
	batchFile="${listFile%.tsv}.batch${batchIdx}.tsv"
	tail -n +"${start}" "${listFile}" | head -n "${SLURM_ARRAY_MAX}" > "${batchFile}"
	batchN=$(wc -l < "${batchFile}")
	jobId=$(sbatch --parsable \
	    --job-name="${jobName}" \
	    --partition="${SLURM_PARTITION}" \
	    --account="${SLURM_ACCOUNT}" \
	    --time="${walltime}" \
	    --ntasks=1 --cpus-per-task="${RCLONE_CPUS}" --mem="${SLURM_MEM}" \
	    --array="1-${batchN}%${MAX_PARALLEL}" \
	    --output="${STATE_DIR}/logs/%x_%A_%a.out" \
	    --export="ALL,GMN_HOME=${GMN_HOME},GMN_CONF=${GMN_CONF}" \
	    ${dependency} \
	    "${sbatchScript}" "${year}" "${batchFile}" "$@") || { gmnLog "ERROR: sbatch failed for ${batchFile}"; return 1; }
	gmnLog "Submitted ${jobName} array job ${jobId} (${batchN} tasks, list ${batchFile})"
	GMN_JOB_IDS+=("${jobId}")
	dependency="--dependency=afterany:${jobId}"
	start=$(( start + SLURM_ARRAY_MAX ))
	batchIdx=$(( batchIdx + 1 ))
    done
}

#----------------------------------------------------
### gmnJobsQueued(): true if any of this user's jobs named $1 are queued or
### running - submitters refuse to (re)plan a year while its jobs are live.
#----------------------------------------------------
gmnJobsQueued() {
    [ -n "$(squeue -h -u "${USER}" -n "${1}" -o %i 2>/dev/null)" ]
}

#----------------------------------------------------
### gmnRotateLog(): moves an existing per-station log to logs/old/ so a fresh
### run starts with an empty log (the watcher reads state from it).
# params: $1 log file
#----------------------------------------------------
gmnRotateLog() {
    local logf=${1}
    [ -e "${logf}" ] || return 0
    mkdir -p "$(dirname "${logf}")/old"
    mv "${logf}" "$(dirname "${logf}")/old/$(basename "${logf}").$(date +%Y%m%d_%H%M%S)"
}

#----------------------------------------------------
### gmnWatch(): foreground progress monitor for the jobs in GMN_JOB_IDS,
### meant to be left running in tmux. A station's state comes from its log:
### no log = queued, last timestamped (gmnLog) line "DONE: ..." / "FAILED: ..."
### = finished, anything else = running in that phase. A station still not
### finished once its jobs have left the queue died (walltime/OOM/scancel).
### Prints every state/phase change as it happens, the last error line for
### failures, a status line on each change and every WATCH_HEARTBEAT seconds
### (with rclone progress of running stations), and a final summary.
# params: $1 watch file (label<TAB>log file per station)
# Returns: 0 if every station is DONE, 1 otherwise.
#----------------------------------------------------
gmnWatch() {
    local watchFile=${1}
    local jobList label logf state phase live err stats now
    local nTotal nDone nFail nRun nQueue changed lastBeat
    local -A seenState=() seenPhase=()
    local failed=() running=()
    jobList=$(IFS=,; echo "${GMN_JOB_IDS[*]}")
    nTotal=$(wc -l < "${watchFile}")
    lastBeat=$(date +%s)
    gmnLog "Watching ${nTotal} stations (jobs ${jobList}), polling every ${WATCH_INTERVAL}s."
    gmnLog "Ctrl-C only stops watching - the jobs keep running (scancel ${jobList//,/ } to stop them)."
    while true
    do
	live=$(squeue -h -j "${jobList}" -o %i 2>/dev/null | wc -l)
	nDone=0; nFail=0; nRun=0; nQueue=0; changed=FALSE
	failed=(); running=()
	while IFS=$'\t' read -r label logf
	do
	    state=QUEUED
	    phase=""
	    if [ -s "${logf}" ]
	    then
		phase=$(grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8} ' "${logf}" | tail -1 | cut -c21-)
		case ${phase} in
		    DONE:*) state=DONE ;;
		    FAILED:*) state=FAILED ;;
		    *) state=RUNNING ;;
		esac
	    fi
	    if [ "${live}" -eq 0 ] && { [ "${state}" = QUEUED ] || [ "${state}" = RUNNING ]; }
	    then
		state=DIED
	    fi

	    if [ "${state}" != "${seenState[${label}]:-}" ] \
		|| { [ "${state}" = RUNNING ] && [ "${phase}" != "${seenPhase[${label}]:-}" ]; }
	    then
		changed=TRUE
		case ${state} in
		    RUNNING) gmnLog "  .. ${label}: ${phase}" ;;
		    DONE) gmnLog "  OK ${label}: ${phase#DONE: }" ;;
		    FAILED)
			err=$(grep -iE 'error|denied|fail' "${logf}" | grep -vE '^srun:|FAILED:' | tail -1)
			gmnLog "  XX ${label}: ${phase}"
			[ -n "${err}" ] && gmnLog "     last error: ${err}"
			gmnLog "     log: ${logf}"
			;;
		    DIED)
			gmnLog "  XX ${label}: job ended without DONE/FAILED (walltime, OOM or scancel?)"
			gmnLog "     log: ${logf} (and the matching %x_%A_%a.out next to it)"
			;;
		esac
		seenState[${label}]=${state}
		seenPhase[${label}]=${phase}
	    fi

	    case ${state} in
		DONE) nDone=$((nDone + 1)) ;;
		FAILED|DIED) nFail=$((nFail + 1)); failed+=("${label}  (${logf})") ;;
		RUNNING) nRun=$((nRun + 1)); running+=("${label}"$'\t'"${logf}") ;;
		QUEUED) nQueue=$((nQueue + 1)) ;;
	    esac
	done < "${watchFile}"

	now=$(date +%s)
	if [ "${changed}" = TRUE ] || [ $(( now - lastBeat )) -ge "${WATCH_HEARTBEAT}" ]
	then
	    gmnLog "Status: ${nDone}/${nTotal} done, ${nRun} running, ${nQueue} queued, ${nFail} failed"
	    if [ "${changed}" = FALSE ]
	    then
		for label in "${running[@]}"
		do
		    logf=${label#*$'\t'}
		    stats=$(grep 'NOTICE:' "${logf}" | tail -1 | sed 's/.*NOTICE: *//')
		    gmnLog "  .. ${label%%$'\t'*}: ${seenPhase[${label%%$'\t'*}]} ${stats:+[${stats}]}"
		done
	    fi
	    lastBeat=${now}
	fi

	[ "${live}" -eq 0 ] && break
	sleep "${WATCH_INTERVAL}"
    done

    gmnLog "=====[ finished: ${nDone}/${nTotal} done, ${nFail} failed ]====="
    if [ ${nFail} -gt 0 ]
    then
	for label in "${failed[@]}"
	do
	    gmnLog "  failed: ${label}"
	done
	gmnLog "Fix the cause and rerun the same command - finished stations are skipped."
	return 1
    fi
    return 0
}
