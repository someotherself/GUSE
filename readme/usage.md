Table of contents:

- [How to install and run guse](#Running)

- [Find commit history of a file](#Commit-history)

- [Running a commit](#Compiling)

See also:
- git and Commit files [git/commits](git_and_commit_files.md)
- How to run a guse chase [CHASE](chase.md)

# Running

## Install the app

```bash
cargo install guse
```

## Flags available

```
-m          - Sets the mount point. Always required.
-s          - Read only. Set the filesystem read-only.
            - Default to false.
-r          - Allow root. Allow the root user to access filesystem.
            - Default to false.
-o          - Allow other. Allow other users to access filesystem.
            - Default to false.
-v          - Verbose. Can be used multiple times to set logging level
            0 -> info (-vv)
            1 -> debug (-v)
            _ -> trace (default)
-t          - Disable the UnixSocket. Will also disable the guse commands.
```

guse will create 2 folders. In this example they are called:
- MOUNT: This is where the filesystem is mounted. This is what the user interacts with when the app is running. MOUNT will be empty when the app is not running.
- data_dir: This is where the files are stored. Commands ran on MOUNT, will be applied on files in data_dir. Running cli commands directly in data_dir wil bypass the FUSE implemantation.

## Run the app

```bash
guse run MOUNT data_dir -o -vv
```

## How to create repositories

There are 2 ways to start new reposities.
### 1. Empty repositories
Open a new cli after starting the app
```bash
cd MOUNT
mkdir my_new_repository
ls my_new_repository
live
cd live
```

```
Inside the new repository there will only be a folder called "live". The live folder will contain an empty, initialized git repository.
Inside the "live" folder is where all the user files and folders can be created normally. The user only has write permissions inside the live folder.
```

### 2. Fetching
A Github or Gitlab repository can be fetched into a new repo.
The url must be properly formatted. Example:

```
URL: https://github.com/tokio-rs/tokio.git
bash: mkdir github.tokio-rs.tokio.git
URL: https://gitlab.com/commento/commento.git
bash: mkdir gitlab.commento.commento.git
```

This fetches the following:
```
+------------------------+---------------------------------+
| Remote ref pattern     | Stored locally under            |
+------------------------+---------------------------------+
| refs/heads/*           | refs/remotes/upstream/*         |   → all branches
| refs/tags/*            | refs/tags/*                     |   → all tags
| HEAD                   | refs/remotes/upstream/HEAD      |   → remote HEAD
| refs/pull/*/head       | refs/remotes/upstream/pr/*      |   → GitHub PR branch tips
| refs/pull/*/merge      | refs/remotes/upstream/pr-merge/*|   → GitHub PR merge commits
+------------------------+---------------------------------+
```

Inside the new repo, the commits will be grouped by MONTH. The MONTH folders, by default, will contains commits from main/master. They do not change if a different branch is checked out in /live.
Inside the MONTH folders, each commit will be found in a directory. The folder name followed the format:
```
Snap001_6fd06aa
001 -> Consecutive numbers, in order of commit time (old to new)
6fd06aa -> Commit HASH (Oid)
```
Optionally, there can be the following folders: Branches, Pr, PrMerge, Tags. These are only created if they exist in the remote.
```
Pr: Only open Pr's will show and will contain folder with number Pr number as name. Each folder contains the commits made on the Pr, since the branch was created.
PrMerge: Contains a folder for each open Pr, but will only contain the syntethic commit created by Github.
Branches: Contains a folder for each branch with the name of the branch. Contains all the commits made on the branch since it was created. A git merge-base is done with main/master and then with the other branches as fall back.
Tags: Contains a folder for each tag, with the name of the tag. Only the single tag commit can be found inside.
```

```bash
Open a new cli after starting the app
cd MOUNT
mkdir github.tokio-rs.tokio.git
ls tokio
2016-07  2017-05  2018-03  2019-01  2019-11  2020-09  2021-07  2022-05  2023-03  2024-01  2024-11  2025-09
2016-08  2017-06  2018-04  2019-02  2019-12  2020-10  2021-08  2022-06  2023-04  2024-02  2024-12  Branches
2016-09  2017-07  2018-05  2019-03  2020-01  2020-11  2021-09  2022-07  2023-05  2024-03  2025-01  Pr
2016-10  2017-08  2018-06  2019-04  2020-02  2020-12  2021-10  2022-08  2023-06  2024-04  2025-02  PrMerge
2016-11  2017-09  2018-07  2019-05  2020-03  2021-01  2021-11  2022-09  2023-07  2024-05  2025-03  live
2016-12  2017-10  2018-08  2019-06  2020-04  2021-02  2021-12  2022-10  2023-08  2024-06  2025-04  Tags
2017-01  2017-11  2018-09  2019-07  2020-05  2021-03  2022-01  2022-11  2023-09  2024-07  2025-05
2017-02  2017-12  2018-10  2019-08  2020-06  2021-04  2022-02  2022-12  2023-10  2024-08  2025-06
2017-03  2018-01  2018-11  2019-09  2020-07  2021-05  2022-03  2023-01  2023-11  2024-09  2025-07
2017-04  2018-02  2018-12  2019-10  2020-08  2021-06  2022-04  2023-02  2023-12  2024-10  2025-08
cd 2021-02
ls
Snap001_cc97fb8  Snap006_0a04954  Snap011_58bd242  Snap016_e3f2dcf  Snap021_52457dc  Snap026_017a483
Snap002_3e5a0a7  Snap007_23fdc2b  Snap012_e827829  Snap017_36bcfa6  Snap022_7de18af  Snap027_5756a00
Snap003_77ca8a9  Snap008_d41882e  Snap013_469b43d  Snap018_6fd06aa  Snap023_8efed43  Snap028_d2ad7af
Snap004_1c1e0e3  Snap009_572a897  Snap014_4099bfd  Snap019_36d7dab  Snap024_c9d2a36  Snap029_fd93ecf
Snap005_fcb6d04  Snap010_6fd9084  Snap015_7c6a1c4  Snap020_53558cb  Snap025_112e160
cd Snap008_d41882e
ls
CODE_OF_CONDUCT.md  LICENSE      benches   stress-test        tokio         tokio-test
CONTRIBUTING.md     README.md    bin       tests-build        tokio-macros  tokio-util
Cargo.toml          SECURITY.md  examples  tests-integration  tokio-stream
```

## Commit summary

```
Before opening the MONTH and Snap folders, a quick summary cam be displayed.
By runnign cat MONTH@, a summary of each folder inside folder will be displayed.
It is necessary that a "@" is added at the end of the folder name.
The summary follows the format:

Commit time     Commit HASH     Folder name     Author name     Commit summary
```

```bash
cd MOUNT
mkdir github.tokio-rs.tokio.git
cd tokio
ls
2016-07  2017-05  2018-03  2019-01  2019-11  2020-09  2021-07  2022-05  2023-03  2024-01  2024-11  2025-09
2016-08  2017-06  2018-04  2019-02  2019-12  2020-10  2021-08  2022-06  2023-04  2024-02  2024-12  live
2016-09  2017-07  2018-05  2019-03  2020-01  2020-11  2021-09  2022-07  2023-05  2024-03  2025-01
2016-10  2017-08  2018-06  2019-04  2020-02  2020-12  2021-10  2022-08  2023-06  2024-04  2025-02
2016-11  2017-09  2018-07  2019-05  2020-03  2021-01  2021-11  2022-09  2023-07  2024-05  2025-03
2016-12  2017-10  2018-08  2019-06  2020-04  2021-02  2021-12  2022-10  2023-08  2024-06  2025-04
2017-01  2017-11  2018-09  2019-07  2020-05  2021-03  2022-01  2022-11  2023-09  2024-07  2025-05
2017-02  2017-12  2018-10  2019-08  2020-06  2021-04  2022-02  2022-12  2023-10  2024-08  2025-06
2017-03  2018-01  2018-11  2019-09  2020-07  2021-05  2022-03  2023-01  2023-11  2024-09  2025-07
2017-04  2018-02  2018-12  2019-10  2020-08  2021-06  2022-04  2023-02  2023-12  2024-10  2025-08
cat 2024-06@
2024-06-04T07:45:35Z    75c953b Snap001_75c953b Weijia Jiang    time: fix big time panic issue (#6612)
2024-06-04T11:34:22Z    8fca6f6 Snap002_8fca6f6 Timo    process: add `Command::as_std_mut` (#6608)
2024-06-04T21:37:13Z    a91d438 Snap003_a91d438 Alan Somers     ci: update FreeBSD CI environment (#6616)
2024-06-04T21:42:42Z    49609d0 Snap004_49609d0 Emil Loer       test: make `Spawn` forward `size_hint` (#6607)
2024-06-04T22:29:28Z    3f397cc Snap005_3f397cc Armillus        io: read during write in `copy_bidirectional` and `copy` (#6532)
2024-06-05T08:20:27Z    16fccaf Snap006_16fccaf John-John Tedro docs: fix docsrs builds with the fs feature enabled (#6585)
2024-06-06T08:08:46Z    8e15c23 Snap007_8e15c23 Russell Cohen   metrics: add `MetricAtomicUsize` for usized-metrics (#6598)
2024-06-07T07:17:25Z    126ce89 Snap008_126ce89 Aaron Schweiger task: implement `Clone` for `AbortHandle` (#6621)
2024-06-07T11:48:56Z    833ee02 Snap009_833ee02 Hai-Hsin        macros: allow `unhandled_panic` behavior for `#[tokio::main]` and `#[tokio::test]` (#6593)
2024-06-08T20:17:06Z    53b586c Snap010_53b586c Rob Ede         task: stabilize `consume_budget` (#6622)
2024-06-09T10:25:54Z    341b5da Snap011_341b5da Conrad Ludgate  metrics: add `spawned_tasks_count`, rename `active_tasks_count` (#6114)
2024-06-10T08:44:45Z    17555d7 Snap012_17555d7 Marek Kuskowski sync: implement `Default` for `watch::Sender` (#6626)
2024-06-12T16:09:59Z    479f736 Snap013_479f736 Niki C         io: improve panic message of `ReadBuf::put_slice()` (#6629)
2024-06-13T06:50:28Z    a865ca1 Snap014_a865ca1 Weijia Jiang    rt: relaxed trait bounds for `LinkedList::into_guarded` (#6630)
2024-06-13T08:58:45Z    53ea44b Snap015_53ea44b Timo            sync: add `CancellationToken::run_until_cancelled` (#6618)
2024-06-14T09:03:47Z    8480a18 Snap016_8480a18 Weijia Jiang    time: avoid traversing entries in the time wheel twice (#6584)
2024-06-15T19:10:47Z    39cf6bb Snap017_39cf6bb FabijanC        macros: typo fix in join.rs and try_join.rs (#6641)
2024-06-15T19:11:35Z    3bf4f93 Snap018_3bf4f93 Uwe Klotz       sync: add `watch::Sender::same_channel` (#6637)
2024-06-17T08:33:08Z    9a75d6f Snap019_9a75d6f Alice Ryhl      metrics: use `MetricAtomic*` for task counters (#6624)
2024-06-21T13:00:33Z    ed4ddf4 Snap020_ed4ddf4 Eric Seppanen   io: fix trait bounds on impl Sink for StreamReader (#6647)
2024-06-23T04:52:51Z    0658277 Snap021_0658277 Hai-Hsin        codec: fix `length_delimited` docs examples (#6638)
2024-06-27T16:10:14Z    65d0e08 Snap022_65d0e08 Tobias Nießen   runtime: fix typo in unhandled_panic (#6660)
2024-06-30T13:02:29Z    68d0e3c Snap023_68d0e3c Alice Ryhl      metrics: rename `num_active_tasks` to `num_alive_tasks` (#6667)

cd 2024-06
cat Snap001_75c953b@
-> Will output the same as git show 75c953b
``` 

## Commit-history

Find commit history of a file

When inside a Snap folder, the cd command can also be used on a file.
This will create a folder with all the versions of that file from the commit history.

If this folder is then opened in an IDE, git will attempt to show the line diffs for each file.

```bash
cd MOUNT
mkdir github.tokio-rs.tokio.git
cd 2025-01/Snap023_21a13f9/tokio/src/net/
ls
addr.rs  lookup_host.rs  mod.rs  tcp  udp.rs  unix  windows
cd udp.rs@
ls
0001_02-08-2016_c458e23.rs  0036_30-01-2018_ae627db.rs  0071_06-01-2020_dcfa895.rs  0106_31-12-2021_49a9dc6.rs
0002_03-08-2016_5f9185e.rs  0037_31-01-2018_a616220.rs  0072_23-01-2020_a70f720.rs  0107_08-01-2022_cb9a68e.rs
0003_12-08-2016_9911f42.rs  0038_06-02-2018_73b763f.rs  0073_12-04-2020_1e67974.rs  0108_10-01-2022_bcb968a.rs
0004_13-08-2016_8327d32.rs  0039_06-02-2018_ad8338e.rs  0074_29-05-2020_c624cb8.rs  0109_25-03-2022_a8b75db.rs
0005_15-08-2016_62f3066.rs  0040_28-02-2018_1190176.rs  0075_28-07-2020_0366a3e.rs  0110_06-04-2022_83477c7.rs
0006_17-08-2016_293d104.rs  0041_02-03-2018_7db7719.rs  0076_27-08-2020_d9d909c.rs  0111_25-07-2022_c0746b6.rs
0007_18-08-2016_12a05b9.rs  0042_06-03-2018_aa4b1b4.rs  0077_12-09-2020_8d2e3bc.rs  0112_30-07-2022_5ab6aaf.rs
0008_21-08-2016_9c309af.rs  0043_06-03-2018_5555cbc.rs  0078_23-09-2020_f25f12d.rs  0113_09-08-2022_255c1f9.rs
0009_24-08-2016_e71d509.rs  0044_13-03-2018_64435f5.rs  0079_28-09-2020_078d0a2.rs  0114_10-08-2022_aea0947.rs
0010_01-09-2016_02538d0.rs  0045_14-05-2018_1f5bb12.rs  0080_01-10-2020_7ec6d88.rs  0115_03-10-2022_6bdcb81.rs
0011_01-09-2016_3282b3e.rs  0046_14-05-2018_6598334.rs  0081_05-10-2020_242ea01.rs  0116_31-10-2022_df99428.rs
0012_07-09-2016_93c61bb.rs  0047_06-08-2018_e964c41.rs  0082_06-10-2020_fcdf934.rs  0117_05-11-2022_f464360.rs
0013_07-09-2016_e60002b.rs  0048_21-02-2019_ab595d0.rs  0083_12-10-2020_8880222.rs  0118_28-11-2022_939b5bb.rs
0014_09-09-2016_8f92dc9.rs  0049_10-05-2019_79d8820.rs  0084_16-10-2020_3cc6ce7.rs  0119_05-12-2022_a1316cd.rs
0015_09-09-2016_00fb7ea.rs  0050_26-06-2019_3cc33dc.rs  0085_22-10-2020_adf822f.rs  0120_06-01-2023_31c7e82.rs
0016_22-10-2016_37a2bed.rs  0051_26-06-2019_0784dc2.rs  0086_23-10-2020_e804f88.rs  0121_08-03-2023_002f4a2.rs
0017_22-11-2016_56c2c31.rs  0052_08-07-2019_8b49a1e.rs  0087_26-10-2020_a9da220.rs  0122_11-03-2023_8eb94a3.rs
0018_22-11-2016_0d10b0e.rs  0053_15-07-2019_ca708d6.rs  0088_27-10-2020_fe2b997.rs  0123_12-03-2023_bfc4379.rs
0019_07-03-2017_8fecf98.rs  0054_20-07-2019_1b2d997.rs  0089_06-11-2020_47658a6.rs  0124_16-03-2023_f177aad.rs
0020_12-05-2017_9e80c82.rs  0055_22-07-2019_59bc364.rs  0090_10-11-2020_d869e16.rs  0125_21-03-2023_2dfe4e8.rs
0021_30-07-2017_77b0ee0.rs  0056_25-07-2019_f311ac3.rs  0091_16-11-2020_d0ebb41.rs  0126_28-03-2023_663e56e.rs
0022_16-08-2017_6090e22.rs  0057_05-08-2019_6d8cc4e.rs  0092_10-12-2020_4b1d76e.rs  0127_21-07-2023_a58beb3.rs
0023_24-08-2017_e8617ea.rs  0058_15-08-2019_7b6438a.rs  0093_12-12-2020_df20c16.rs  0128_27-07-2023_c445e46.rs
0024_11-09-2017_8a43472.rs  0059_15-08-2019_d0a8e5d.rs  0094_15-12-2020_fcce78b.rs  0129_28-07-2023_6aca07b.rs
0025_13-09-2017_317c115.rs  0060_15-08-2019_d8b23ef.rs  0095_02-01-2021_56272b2.rs  0130_16-10-2023_1b8ebfc.rs
0026_05-10-2017_259c7a0.rs  0061_18-08-2019_08b07af.rs  0096_20-01-2021_6f8a4d7.rs  0131_27-01-2024_e53b92a.rs
0027_27-10-2017_39173f8.rs  0062_28-08-2019_de9f05d.rs  0097_06-02-2021_6fd9084.rs  0132_08-02-2024_0fbde0e.rs
0028_27-10-2017_ca8104a.rs  0063_21-10-2019_978013a.rs  0098_12-02-2021_4099bfd.rs  0133_27-08-2024_479a56a.rs
0029_01-12-2017_0b54557.rs  0064_21-10-2019_b8cee1a.rs  0099_29-06-2021_57c90c9.rs  0134_29-12-2024_970d880.rs
0030_05-12-2017_259996d.rs  0065_24-10-2019_03a9378.rs  0100_30-06-2021_90e1935.rs  0135_30-12-2024_b3ff911.rs
0031_05-12-2017_8fcce95.rs  0066_15-11-2019_9306795.rs  0101_26-07-2021_afb734d.rs  0136_04-01-2025_2353806.rs
0032_05-12-2017_108e1a2.rs  0067_19-11-2019_7c8b887.rs  0102_24-09-2021_7875f26.rs  0137_21-01-2025_21a13f9.rs
0033_12-12-2017_849771e.rs  0068_20-11-2019_d4fec2c.rs  0103_19-10-2021_095012b.rs
0034_05-01-2018_dac13c1.rs  0069_20-11-2019_15dce2d.rs  0104_31-12-2021_0190831.rs
0035_30-01-2018_117dcba.rs  0070_23-11-2019_3ecaa6d.rs  0105_31-12-2021_96370ba.rs
```

# Deleting a repo

### !This operation is permanent and does not require confirmation!

This operation only works when the app us running

```bash
guse repo remove tokio
```

# Updating a repo

A new fetch can be done on an existing repo to update it.

This operation only works when the app us running

```bash
guse repo update tokio
```

By default, this will use the remote name created by guse (upstream). A custom remote can be added optionally:

```bash
guse repo update tokio origin
```

# Compiling

The snap folders also have write permissions.

### This is a temporary write location. Any files will be cleared at the start of the filesystem.

Each commit can be compiled or ran, and tests can be performed.

```bash
cd MOUNT
mkdir mkdir github.someotherself.guse.git
cd guse/2025-09/Snap140_4e66c30/
ls 
Cargo.lock  Cargo.toml  LICENSE  README.md  readme  src
cargo test
   Compiling proc-macro2 v1.0.95
   Compiling libc v0.2.175
   Compiling pkg-config v0.3.32
   Compiling vcpkg v0.2.15
   Compiling writeable v0.6.1
   Compiling rustversion v1.0.21
   ....

```