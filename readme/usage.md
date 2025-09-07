# How to install and run GUSE

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
```

GUSE will create 2 folders. In this example they are called:
- MOUNT: This is where the filesystem is mounted. This is what the user interacts with when the app is running. MOUNT will be empty when the app is not running.
- data_dir: This is where the files are stored. Commands ran on MOUNT, will be applied on files in data_dir. Running cli commands directly in data_dir wil bypass the FUSE implemantation.

## Run the app

```bash
guse run MOUNT data_dir -o -vv
```

## How to create repositories

There are 2 ways to start new reposities.
### 1. Empty repositories
```bash
Open a new cli after starting the app
cd MOUNT
mkdir my_new_repository
ls my_new_repository
live
cd live
```

```
Inside the new repository there will only be a folder called "live".
Inside the "live" folder is where all the user files and folders can be created normally. The user only has write permissions inside the live folder.

Any snapshots (commits/not implemented) wil be displayed in the same folder as live.
```

### 2. Fetched repositories
A Github or Gitlab repository can be fetched into a new repo.
The url must be properly formatted. Example:

```
URL: https://github.com/tokio-rs/tokio.git
bash: mkdir github.tokio-rs.tokio.git
```

Inside the new repo, the commits will be grouped by MONTH.
Inside the MONTH folders, each commit will be found in a directory. The folder name followed the format:
```
Snap001_6fd06aa
001 -> Consecutive numbers, in order of commit time
6fd06aa -> Commit HASH (Oid) as 
```

```bash
Open a new cli after starting the app
cd MOUNT
mkdir github.tokio-rs.tokio.git
ls tokio
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
2024-06-08T20:17:06Z    53b586c Snap010_53b586c Rob Ede task: stabilize `consume_budget` (#6622)
2024-06-09T10:25:54Z    341b5da Snap011_341b5da Conrad Ludgate  metrics: add `spawned_tasks_count`, rename `active_tasks_count` (#6114)
2024-06-10T08:44:45Z    17555d7 Snap012_17555d7 Marek Kuskowski sync: implement `Default` for `watch::Sender` (#6626)
2024-06-12T16:09:59Z    479f736 Snap013_479f736 Niki C  io: improve panic message of `ReadBuf::put_slice()` (#6629)
2024-06-13T06:50:28Z    a865ca1 Snap014_a865ca1 Weijia Jiang    rt: relaxed trait bounds for `LinkedList::into_guarded` (#6630)
2024-06-13T08:58:45Z    53ea44b Snap015_53ea44b Timo    sync: add `CancellationToken::run_until_cancelled` (#6618)
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

## Find commit history of a file

When inside a Snap folder, the cd command can also be used on a file.
This will create a folder with all the versions of that file from the commit history.

```bash
cd MOUNT
mkdir github.tokio-rs.tokio.git
cd 2025-01/Snap023_21a13f9/tokio/src/net/
ls
addr.rs  lookup_host.rs  mod.rs  tcp  udp.rs  unix  windows
cd udp.rs@
ls
0001_8713d39228923bd0acbebe7df817ecb5e7d69cad.rs  0070_d4fec2c5d628b180226f6ab3005aa3e5845f1929.rs
0002_2353806dafd25adef874b69364bb826da4bbffd2.rs  0071_7c8b8877d440629ab9a27a2c9dcef859835d3536.rs
0003_b3ff911c389405a5fc2fb931517449c26b252d56.rs  0072_930679587ae42e4df3113159ccf33fb5923dd73a.rs
0004_970d880ceb473b222a9ddd4b35b934ca68cecb4a.rs  0073_03a9378297c73c2e56a6d6b55db22b92427b850a.rs
0005_479a56a010d25f86207841ec4fcc685402addcad.rs  0074_b8cee1a60ad99ef28ec494ae4230e2ef4399fcf9.rs
0006_0fbde0e94b06536917b6686e996856a33aeb29ee.rs  0075_978013a215ebae63cd087139514de32bbd36ce11.rs
0007_e53b92a9939565edb33575fff296804279e5e419.rs  0076_de9f05d4d3325a281ddb40adf103fa2b4bba7ee6.rs
0008_1b8ebfcffb10beadda709ea4edfc1078a9897936.rs  0077_08b07afbd9beb8d92c7aeb0cf07e56d065a73726.rs
0009_6aca07bee745c8a1f8ddc1c0f27732dec9d3cdb2.rs  0078_d8b23ef85235b9efef9a52ad7933dd3e3c0b6958.rs
0010_c445e467ce4363b3a9b6825268814a9bc27c0127.rs  0079_d0a8e5d6f2921fadc51a9612f6fe558e4213560f.rs
0011_a58beb3aca18e6ec4cb444b6c78d5a3373742998.rs  0080_7b6438a17247e026c996712a7c83d0c43442d73e.rs
0012_663e56e983a2fdbd2d9c51c77d49745a74aada70.rs  0081_6d8cc4e4755abbd0baac9abf154837b9be011a07.rs
0013_2dfe4e8885647378343011006bce860a1675d8e6.rs  0082_f311ac3d4faa4fa1203ad5586a7676604ffe7736.rs
0014_f177aad6e4d141fe412bb8a16d96b2dc32a688df.rs  0083_59bc364a0e71ace4e819a3e5f5048ce2bc388ed4.rs
0015_bfc43795f994c5f019e084ff88ab6d0960e2a171.rs  0084_1b2d997863709a3d5cb1e2dc78048d7e6566a17f.rs
0016_8eb94a33c078831a4e0680bf59e6ea6aefa5d970.rs  0085_ca708d6d8783b4fc86ccc059fb7a40e14edfe812.rs
0017_002f4a28c882d127a665bb8d71f751d4eb5e1b22.rs  0086_8b49a1e05fa8d070c4d9582beb7491b284f1556a.rs
0018_31c7e8291993f42b27f5306fd0c33848c9fc796f.rs  0087_0784dc27679beecdb06f273ea8c8af0168212c12.rs
0019_a1316cd792596baa079144bf4672f59e99556531.rs  0088_3cc33dca7c9a63ce1a54593c3bb1258cdaff7a1c.rs
0020_939b5bb42f36981ad1dc7dd7a7942718f2d61a5f.rs  0089_79d88200500f6e6c9970e1ad26469276c1a2f71f.rs
0021_f4643608adddcf80dea03965d38347e91b71bc87.rs  0090_ab595d08253dd7ee0422144f8dafffa382700976.rs
0022_df99428c17ff03134d8f081ee80ef0f6fbe3c813.rs  0091_e964c4136c91c535fb2dfee0a3327dc5a6599903.rs
0023_6bdcb813c66b1ed720b0801171685de69a983dd1.rs  0092_6598334021ee281f484492c4acc75571d82f046b.rs
0024_aea09478e1d3c7dc250ccdec87268446dd35c5d7.rs  0093_1f5bb121e29997bf404beab5f6485baff878a700.rs
0025_255c1f95b7ab994b88c2a864ba5ff63b053677d8.rs  0094_64435f5b35efa761a8c3bf67f599e01b27f9d0a6.rs
0026_5ab6aaf3cdc54d348ab73c66350f3f8e1ef7f96e.rs  0095_5555cbc85e48a110f7d7d60ba6af9ec31eb17142.rs
0027_c0746b6a300f558410e719e27d2d4855f5407262.rs  0096_aa4b1b431115a858fff3e8c2b7c67b2e0e9f603b.rs
0028_83477c725acbb6a0da54afc26c67a8bd57e3e0b9.rs  0097_7db77194194851fcc7cad4d68f0481941fb8a285.rs
0029_a8b75dbdf4360c9fd7fb874022169f0c00d38c4a.rs  0098_1190176be7912db327f5e2784e51ce87c385201b.rs
0030_bcb968af8494d2808c2986104d6e67d0b276b1fe.rs  0099_ad8338e4da63f659acce89284381d08a2474f85b.rs
0031_cb9a68eb1ac15a9b8c62915e3fed2ec3ef1e1e2c.rs  0100_73b763f69fe517fdbbb0360bd9c0a50db8f8f62c.rs
0032_49a9dc6743a8d90c46a51a42706943acf39a5d85.rs  0101_a6162200905494745895bc8c1ba63d42cd7648af.rs
0033_96370ba4ce9ea5564f094354579d5539af8bbc9d.rs  0102_ae627db266600f8d010b6eeb9d1be0fff677f0ce.rs
0034_0190831ec1922047751b6d40554cc4a11cf2a82c.rs  0103_117dcba8cbff8fc5b688865360eb2c04277b09eb.rs
0035_095012b03be44b6f023553292e05fe7d5cf35a98.rs  0104_dac13c1df4a5baa8e7e4c25749585c2d90278af0.rs
0036_7875f26586419af61cedaadde50334bbe6eb285a.rs  0105_849771ecfa1e22fdd4f0bd299d10f0026ce14ed5.rs
0037_afb734d1893a898aea9e8d7eeee05bbe8f22ce1f.rs  0106_108e1a2c1a66a6f0123704e42624b51e9536476f.rs
0038_90e1935c486417ec64507b26ff4bf80a3dfb19e2.rs  0107_8fcce957cd0f8484e5ad078fc4ada244ea463fb0.rs
0039_57c90c9750d02c5bca93a939c7d44d7fe74fe464.rs  0108_259996d8051b1fcdf04042e253c870f206a1926f.rs
0040_4099bfdef05f514d4ca25cf15a58b12e2c53fdc1.rs  0109_0b54557796cee1c1d3a55ca29be982d24e5b3f3b.rs
0041_6fd9084d470aa34e02c9dedcbf52e310fcb9cacc.rs  0110_ca8104ad690b91f8c9efaf628bfc0e144fa4b0d9.rs
0042_6f8a4d7a0b4c7cb0aa8f46f844ff8a47a24bc6fd.rs  0111_39173f8830fbe10cc46cb3c2ad5b53e27561f9e8.rs
0043_56272b2ec739479496183acd07b056c543333324.rs  0112_259c7a08849ecba60502e2a1b7f3c287948c2b34.rs
0044_fcce78b33ad67d0910f01ba4a2e79e5197e97aab.rs  0113_317c11552ca57223bb2fb36e84d143383799bd2e.rs
0045_df20c162ae1308c07073b6a67c8ba4202f52d208.rs  0114_8a43472b35e1807eb047ab01259adbabf0975346.rs
0046_4b1d76ec8f35052480eb14204d147df658bfdfdd.rs  0115_e8617ea1fc0bbe8d06b785d8ce68aa2663ede2d7.rs
0047_d0ebb4154748166a4ba07baa4b424a1c45efd219.rs  0116_6090e221357bdc2c29433fe0fe8a3b91f423b84c.rs
0048_d869e16990c5fc2cbda48b036708efa4b450e807.rs  0117_77b0ee0a47d48d8d67a50310acd69808f3b25553.rs
0049_47658a6da5a6cf2d7db4727e61915709727cd632.rs  0118_9e80c82400b68d89b41ab7f8f82b527f1ca7a3cb.rs
0050_fe2b9976755407b85c82b5cdee9d8c5e16e3d6c6.rs  0119_8fecf98aef1bb2f4f37303c44b2a51126b9c54ff.rs
0051_a9da220923bbd329e367ac31de229cc56d470b8d.rs  0120_0d10b0e05a920f9d8b5d05d8c078419dabad9464.rs
0052_e804f88d60071f0d89db85aaa4a073857904b545.rs  0121_56c2c31bcf545f5715429b6aa6924bde76f06c4d.rs
0053_adf822f5cc11acdeeae3cf119469a19c524e82b4.rs  0122_37a2bed4cfbe49e8e783291dcfd974b28ce95ab9.rs
0054_3cc6ce7a995b0d34b00ca1e5798c2c523cc63e7a.rs  0123_00fb7ea4a3b9e7cd05780f563099f02a37e0b75e.rs
0055_8880222036f37c6204c8466f25e828447f16dacb.rs  0124_8f92dc9d56d0d00d8cd7d9ea30fe69ce59cf1bf3.rs
0056_fcdf9345bf19e9a1e1664f01713f9eba54da27c5.rs  0125_e60002b653594c0a024733d1db0ce8d680e4f808.rs
0057_242ea011891099f348b755b2ea10ec9e9ea104db.rs  0126_93c61bb384b8097a4897661eb877fc6a8440a02a.rs
0058_7ec6d88b21ea3e5531176f526a51dae0a4513025.rs  0127_3282b3ec0d0da1237c11e59ea0ac46407dd3edd5.rs
0059_078d0a2ebc4d4f88cb6bce05c8ac4f5038dae9be.rs  0128_02538d035f8dc65609e75c60188fa31d4c7d23f5.rs
0060_f25f12d57638a2928b3f738b3b1392d8773e276e.rs  0129_e71d509fee767d6b796ba18a5501f80f0fb4babc.rs
0061_8d2e3bc575f51815ae7319f1e43fe6c7d664e6e4.rs  0130_9c309af59776c06c853f2bddf318746eb6618aeb.rs
0062_d9d909cb4c6d326423ee02fbcf6bbfe5553d2c0a.rs  0131_12a05b9568f1579e8855be11a6d1f4060af7c8b5.rs
0063_0366a3e6d1aa4e7bf4a1c717680dd0947589264b.rs  0132_293d1041770384c9eeb34ac7d97214feaf3b88c3.rs
0064_c624cb8ce308aa638d42c2c539439e1db4cfc1c2.rs  0133_62f306629d6f295e37b2a92db6b9219116a5edd3.rs
0065_1e679748ecedfb0e894c5028eb8c67f44e47507a.rs  0134_8327d327c10738517a8ff6c6aa986baa70305ae4.rs
0066_a70f7203a46d471345128832987017612d8e4585.rs  0135_9911f421eba909012cb1856d4d983fc68d34569c.rs
0067_dcfa895b512e3ed522b81b18baf3e33fd78a600c.rs  0136_5f9185ef4c5d17b8431a367d2ea8325307f44904.rs
0068_3ecaa6d91cef271b4c079a2e28bc3270280bcee6.rs  0137_c458e2394048b2571c7feefe544fff6f49189ec7.rs
0069_15dce2d11ad849e25f0336f09fdb1cca7e405a9e.rs
```

# Deleting a repo

### !This operation is permanent and does not require confirmation!

This operation only works when the app us running

```bash
guse repo remove tokio
```