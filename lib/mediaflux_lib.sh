### Mediaflux/pshell datastore helpers for the GMN Acacia <-> Banksia scripts.
###
### Vendored copies (not sourced from /opt/server_tools) so this repo deploys
### to Setonix self-contained. Sources:
###   DEBUG()                               <- /opt/server_tools/lib.sh/dfn_lib.sh
###   checkDatastoreDelegate()              <- /opt/server_tools/lib.sh/dfn_mediaflux_lib.sh
###   checkIfNamespaceExists()              <- /opt/server_tools/lib.sh/dfn_mediaflux_lib.sh
###   createPawseyDatastoreDirIfNotExist()  <- /opt/server_tools/lib.sh/dfn_mediaflux_lib.sh
### Changes vs the originals are marked "GMN:", plus ${0##*} -> ${0##*/}
### in messages and removal of commented-out/unused debug lines. waitForDatastoreDelegate() and
### getIncrementIngestionIDPawsey() are deliberately not copied: the former
### drops into an interactive pshell login (useless inside a batch job - see
### gmnRequireDelegate() in gmn_banksia_lib.sh instead), the latter is
### DFN-specific ingestion bookkeeping.
###
### Requires the caller to have set: PSHELL_INVOKE, isDebug.

DEBUG() {
    if [ "${isDebug}" = "TRUE" ]
    then
	echo "DEBUG: $@"
    fi
}

#----------------------------------------------------
### check LiveARC datastore delegate status
# params: $1 : logfile
#         $2: 0 if OK
#             1 if will expire soon
#             2 if expired or cannot find delegate time
#             3 if failed to connect to server or server timeout
#             4 if got reply from server, but there is some other error
#----------------------------------------------------
checkDatastoreDelegate() {
    local logFile=${1}
    local retVal=4
    ### check LiveARC delegate status
    echo "-----[ check LiveARC delegate status ]----------" | tee -a ${logFile}

    statusOutput=$( ${PSHELL_INVOKE} "whoami" )
    pshellOK=$?
    DEBUG "${statusOutput}"
    if [ ${pshellOK} -ne 0 ]
    then
	echo "ERROR: ${0##*/}:checkDatastoreDelegate() ${statusOutput}" | tee -a ${logFile}
	if [[ "${statusOutput}" =~ (Failed to establish network connection) ]]
	then
	    retVal=3
	elif [[ "${statusOutput}" =~ (session is not valid) ]]
	then
	    retVal=2
	    echo "Delegate token broken - login and re-delegate!" | tee -a ${logFile}
	else
	    # some other error
	    retVal=4
	fi
    elif [[ "${statusOutput}" =~ (delegate) ]]
    then
	### pshell's whoami() (mfclient.py) prints exactly "user = delegate
	### (expires %s)" where %s is either the literal "Never" or a real
	### "DD-Mon-YYYY HH:MM:SS" date.
	if [[ "${statusOutput}" =~ \(expires\ Never\) ]]
	then
	    echo "INFO: Delegate OK - no expiry" | tee -a ${logFile}
	    retVal=0
	elif [[ "${statusOutput}" =~ \(expires\ ([0-9]{1,2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-[0-9]{4}\ [0-9]{2}:[0-9]{2}:[0-9]{2})\) ]]
	then
	    local expireTimeStr="${BASH_REMATCH[1]}"
	    echo "$( date +"%Y-%m-%d %H:%M:%S" ): Delegate ${expireTimeStr}" | tee -a ${logFile}
	    local timeUNIXDelegate=$(date +%s -d "${expireTimeStr}")
	    local timeNow=$(date +%s)
	    local minDaysExpire=3
	    ### GMN: arithmetic compare (original used a [[ > ]] string compare)
	    if (( timeNow + (24*60*60*minDaysExpire) > timeUNIXDelegate ))
	    then
		### GMN: original also sent an email to the DFN list here
		echo "WARNING: Delegate will expire in less than ${minDaysExpire} days - login and re-delegate soon!" | tee -a ${logFile}
		retVal=1
	    else
		echo "Delegate OK" | tee -a ${logFile}
		retVal=0
	    fi
	else
	    echo "WARNING: ${0##*/}:checkDatastoreDelegate() cannot find the delegate expiry time!" | tee -a ${logFile}
	    echo "${statusOutput}" | tee -a ${logFile}
	    retVal=2
	fi
    else
	### whoami succeeded against a regular (non-delegate) session - a
	### valid, working session, just not one that survives long unattended.
	echo "INFO: Session OK (regular login, not a delegate - fine for now, but won't survive long unattended; consider a delegate for batch use)" | tee -a ${logFile}
	retVal=0
    fi

    local  __result=${2}
    eval $__result="'$retVal'"
}

#-------------------------------------------------
### check if namespace exists on Pawsey datastore
# parameter: full path of namespace on pawsey to check if exists
# retval: 0 ..... exists
#         1 ..... does not exist
#         >=2 ... failed to find out
#           2 ... asset.namespace.exists query returned neither true nor false
#           3 ... problems parsing asset.namespace.exists query output
#           4 ... calling pshell failed
#-------------------------------------------------
checkIfNamespaceExists() {
    local pawseyDir=$1

    local REGEXP_EXISTS="((exists=).?(true|false).?)"
    local REGEXP_NAMESPACE="((-namespace=)(.*))"
    local REGEXP_MY_NAMESPACE="((-namespace=).?(${pawseyDir}).?)"

    local ifExistsOut=$(${PSHELL_INVOKE} "asset.namespace.exists :namespace ${pawseyDir}")
    local ifExistsRetVal=$?
    local existsInfo=""
    if [ ${ifExistsRetVal} -eq '0' ]
    then
	if [[ "${ifExistsOut}" =~ ${REGEXP_EXISTS} ]]
	then
	    existsInfo="${BASH_REMATCH[1]}"
	else
	    echo "ERROR: checkIfNamespaceExists(): problem parsing output of asset.namespace.exists query, cannot find \"${REGEXP_EXISTS}\"."
	    echo "DETAILS: ${ifExistsOut}"
	    return 3
	fi
	DEBUG existsInfo=${existsInfo}

	if [[ "${ifExistsOut}" =~ ${REGEXP_NAMESPACE} ]]
	then
	    namespaceInfo="${BASH_REMATCH[1]}"
	else
	    echo "ERROR: checkIfNamespaceExists(): problem parsing output of asset.namespace.exists query, cannot find \"${REGEXP_NAMESPACE}\"."
	    echo "DETAILS: ${ifExistsOut}"
	    return 3
	fi
	DEBUG namespaceInfo=${namespaceInfo}

	if [[ "${namespaceInfo}" =~ ${REGEXP_MY_NAMESPACE} ]]
	then
	    if [[ "${existsInfo}" =~ (true) ]]
	    then
		return 0
	    elif [[ "${existsInfo}" =~ (false) ]]
	    then
		return 1
	    else
		echo "ERROR: checkIfNamespaceExists(): asset.namespace.exists query returned neither true nor false, but \"${existsInfo}\"."
		echo "DETAILS: ${ifExistsOut}"
		return 2
	    fi
	else
	    echo "ERROR: checkIfNamespaceExists(): failed to pass namespace/dir name? Cannot find \"${REGEXP_MY_NAMESPACE}\"."
	    echo "DETAILS: ${ifExistsOut}"
	    return 3
	fi
    else
	echo "ERROR: checkIfNamespaceExists(): calling pshell failed."
	echo "DETAILS: ${ifExistsOut}"
	return 4
    fi
}

#-------------------------------------------------
### create namespace/directory on Pawsey datastore if it does not exist yet
# parameter: full path of namespace on pawsey
# retval: 0 ..... exists or created
#         2-4 ... see checkIfNamespaceExists()
#         other . pshell mkdir failed
#-------------------------------------------------
createPawseyDatastoreDirIfNotExist() {
    local targetDir=${1}
    checkIfNamespaceExists ${targetDir}
    local targetDirExists=$?
    if [ ${targetDirExists} -eq 1 ]
    then
	${PSHELL_INVOKE} "mkdir ${targetDir}"
	return $?
    else
	return ${targetDirExists}
    fi
}
