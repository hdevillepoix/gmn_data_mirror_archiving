#!/bin/bash
#SBATCH --partition=copy
#SBATCH --ntasks=1
### Fallback only: gmnSubmitBatch() passes partition/account/etc on the
### sbatch command line from config/gmn_banksia.conf, which overrides these.
############################################################
# Continuation job for a work list too big to queue in one go (MaxSubmitJobs).
# Runs after the previous batch's array (afterany) and submits the next
# batch, plus another continuation if lines are still left. Carries the
# same job name, so gmnJobsQueued/gmnWatch/scancel -n see the whole chain.
# Logs to the original submit log (GMN_LOG, exported by the submitter).
#
# Args: <first line> <batch index> <job name> <sbatch script> <walltime>
#       <year> <list file> [extra args for the sbatch script...]
############################################################
set -u

source "${GMN_HOME:?GMN_HOME not exported by the submitter}/gmn_banksia_lib.sh"
gmnLoadConf

GMN_JOB_NAMES=()
gmnSubmitBatch "$@"
