# GMN archives: Acacia ⇄ Banksia

Scripts that copy the GMN nightly archives from Acacia (S3, `hdevillepoix:gmn-archives-<year>/<station>/` (rclone remote `ACACIA_REMOTE`)) to Banksia tape (Mediaflux, `/projects/DFN/GMN/<year>/gmn-archives-<year>_<station>.tar`), and back again. They run as Slurm array jobs on Setonix's `copy` partition. Settings live in `config/gmn_banksia.conf`.

## How it works
- **One tar per year-station**, whatever its size. For example, `/projects/DFN/GMN/2018/gmn-archives-2018_us0001.tar` holds `us0001/` with its nightly `.tar.bz2` files. The tar is uncompressed and comes with a `.sha256` sidecar.
- **One Slurm array task per station**, on the `copy` partition, with at most `MAX_PARALLEL` running at once. Each task holds one station on scratch: about 1× its size when archiving and 2× when restoring. Submitters estimate the peak and refuse to run if it is above `SCRATCH_BUDGET_BYTES` (2 TiB).
- **State lives in `STATE_DIR`**, which is persistent and not on scratch:
  - `archive_done.tsv`: `year station tar_bytes sha256 nfiles date job`
  - `restore_done.tsv`: `year station dest tar_only date job`
  - `logs/`: one log per year-station, plus the Slurm `.out` files.
- **Rerunning is safe.** Stations already done are skipped, and failed ones are resubmitted. If an archived station has a different file count on Acacia now, you get a warning; it is not re-archived.

## Stopping
1. Ctrl-C the watcher. This stops only the watching.
2. `scancel -n gmn_arch_<year>` (or `gmn_rest_<year>`).

Cancelled tasks log `FAILED: cancelled` and delete their scratch dir. Nothing is recorded in the ledgers, so rerunning the same command redoes those stations. A tar cut off mid-upload is overwritten when that happens.

## Usage (on a Setonix login node, inside tmux)
The submitters work out what's left to do, submit it, then **stay in the foreground and watch the jobs** until they finish. The output looks like this:

```
2026-10-08 10:02:11   .. 2018 us0001: Downloading 11 files (3.3GB) from hdevillepoix:gmn-archives-2018/us0001
2026-10-08 10:03:41   .. 2018 us0001: Creating gmn-archives-2018_us0001.tar (11 files)
2026-10-08 10:03:58   .. 2018 us0001: Uploading gmn-archives-2018_us0001.tar to /projects/DFN/GMN/2018
2026-10-08 10:05:12   OK 2018 us0001: /projects/DFN/GMN/2018/gmn-archives-2018_us0001.tar (3.3GB)
2026-10-08 10:05:12   XX 2018 us0003: FAILED: rclone copy
2026-10-08 10:05:12      last error: ... AccessDenied: ...
2026-10-08 10:05:12 Status: 1/8 done, 3 running, 3 queued, 1 failed
```

What it prints:
- Every time a station changes phase.
- Each failure, with its last error line.
- A status line on every change, and every `WATCH_HEARTBEAT` seconds with rclone progress.
- A final summary.

Ctrl-C stops only the watching; the jobs keep running. Use `--no-watch` to submit and exit straight away. The output is also written to `state/logs/*_submit_*.log`.

Before submitting anything, the archive submitter checks that it can actually read an object from the bucket, not just list the bucket.

```bash
./install_gmn_banksia.sh                       # from here, rsyncs to Setonix

./gmn_banksia_archive_submit.sh --dry-run 2018 # what would run + scratch estimate
./gmn_banksia_archive_submit.sh 2018 2019
./gmn_banksia_archive_submit.sh --all          # every gmn-archives-YYYY bucket

./gmn_banksia_restore_submit.sh --dry-run 2018 us0001
./gmn_banksia_restore_submit.sh 2018                                   # back to the original bucket
./gmn_banksia_restore_submit.sh --dest hdevillepoix:gmn-restore-test 2018 us0001
./gmn_banksia_restore_submit.sh --tar-only --dest hdevillepoix:gmn-tars 2018
```
- Restores upload with `rclone --immutable`, so an existing object with different content is never overwritten.
- Set `KEEP_SCRATCH_ON_FAILURE=TRUE` in the environment to keep a failed task's scratch dir for debugging.

## To confirm on first use
- `SLURM_ACCOUNT` is a valid compute allocation for the `copy` partition.
- The Mediaflux delegate can create `/projects/DFN/GMN`.
- How `gmnRemoteAssetSize` parses the output of `pshell "asset.get :id path=... :xpath -ename size content/size"`.
- How `pshell get` behaves when content is offline on tape: does it wait for recall, or fail so it needs a retry?

`lib/mediaflux_lib.sh` holds copies of helpers from `/opt/server_tools/lib.sh/dfn_mediaflux_lib.sh`. Each change from the original is marked `GMN:`.
