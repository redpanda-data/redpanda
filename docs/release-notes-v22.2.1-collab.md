PRs that have empty release notes section

    Add namespace to labels for metrics on public endpoint by @VladLazar in #5469
    Add partition size to health monitor by @ZeDRoman in #5010
    Added consumer group migration test when group topic is not present by @mmaslankaprv in #4547
    Added slash command to create pull requests by @gousteris in #4143
    Additional detail in config error message by @travisdowns in #5213
    Backport Slash Command Refactor by @nk-87 in #4961
    Change alter-config help text to use --delete by @travisdowns in #5042
    Clarify sasl mechanism flag usage and fix bracketing by @larsenpanda in #5038
    Cmake: bump seastar version by @ztlpn in #4503
    Extend lifetime of group for background tasks by @travisdowns in #5119
    Fix for bug where some requests would not respect min/max version checks by @graphcareful in #5308
    Fix for kafka/protocol unit tests not running by @graphcareful in #4584
    Fixed printing group instance id in rpk group describe by @mmaslankaprv in #4644
    KIP-525 - Return topic configs in create topics responses by @graphcareful in #5133
    Nit fixes: unused imports, stale comments, py annotations, clang tidy polish. by @ajfabbri in #4896
    Operator: execute centralized configuration operations always on the same node by @nicolaferraro in #5125
    Partition autobalancer planner by @ZeDRoman in #5142
    Partition autobalancer planner cancel movement to unavailable nodes by @ZeDRoman in #5365
    Pass node report by reference by @travisdowns in #5217
    Per-handler memory estimation, more accurate estimate for metadata handler by @travisdowns in #5346
    Revert "build(deps): bump jackson-databind in /tests/java/e2e-verifiers" by @NyaliaLui in #5475
    Strip license key of whitespace and newlines by @graphcareful in #5401
    Temporarily disable coproc on ci by @graphcareful in #4557
    Updated client swarm version by @mmaslankaprv in #4940
    admin_server: redact configs in admin API by @andrwng in #5024
    application: handle unwanted positional args by @abhijat in #4941
    build(deps): bump async from 3.2.0 to 3.2.4 in /src/js by @dependabot in #5271
    build(deps): bump jackson-databind from 2.13.1 to 2.13.2.1 in /tests/java/e2e-verifiers by @dependabot in #5266
    build: install unzip and support manjaro by @balusch in #4508
    build: simplify admin server json handling by @dotnwat in #4400
    c/persisted_stm: suppress error when offset monitor wait was aborted by @mmaslankaprv in #5342
    c/types: removed unused sort from get_metadata by @mmaslankaprv in #4323
    chore: tools/ readability improvements by @ryanrussell in #5235
    cloud_roles/tests: return UTC time in test script by @abhijat in #5398
    cloud_roles: adds minio credentials to mock iam server by @abhijat in #5402
    cloud_roles: optionally enable tls in http client by @abhijat in #5497
    cloud_storage: handle fail to write cache item by @LenaAn in #5364
    cloud_storage: redacts fields from header by @abhijat in #4939
    cluster: recheck raft0 leader existence under semaphore by @travisdowns in #4913
    cmake: adds option to enable distcc by @abhijat in #5218
    cmake: bump seastar tag by @VladLazar in #5508
    cmake: fail when distcc is enabled but not found by @abhijat in #5273
    cmake: update seastar tag for OSS build by @VladLazar in #5436
    ducktape: GroupMetricsTest check if leader exists by @ZeDRoman in #4442
    ducktape: add OMB to ducktape by @ZeDRoman in #4161
    ducktape: add clustered ducktape settings to omb by @ZeDRoman in #4754
    ducktape: change omb branch to main by @ZeDRoman in #5000
    ducky/FranzGoServices: Add batchmaxbytes option by @NyaliaLui in #4472
    ducky/FranzGoTest: add a matrix of config options by @NyaliaLui in #4484
    ducky: do common ops while the system is busy by @NyaliaLui in #4394
    ducky: include support for S3 by @NyaliaLui in #4380
    expiring_promise: fix loss of exception information by @dotnwat in #4895
    http: tests and improvements for header logging by @abhijat in #5454
    k/fetch: validate fetch offset against high watermark by @mmaslankaprv in #4201
    k8s/configurator: remove unneeded log stmnt by @0x5d in #4652
    k8s: Update by @RafalKorepta in #5028
    kafka: no more special treatment for unknown_server_error code. by @balusch in #5232
    md-viewer/controller: added support for centralized configuration by @mmaslankaprv in #4922
    metrics: replicate selected seastar metrics by @VladLazar in #5434
    operator: add debug info to kuttl tests and fix by @nicolaferraro in #4383
    operator: add make task for e2e-unstable tests by @nicolaferraro in #4748
    operator: avoid logging unnecessary information in centralized config… by @nicolaferraro in #4674
    operator: fix possible conflicts in controller tests by @nicolaferraro in #4340
    operator: move centralized-config kuttl test to e2e-unstable by @nicolaferraro in #4643
    operator: use relaxed kuttl assertions in e2e tests to reduce flakiness by @nicolaferraro in #5111
    perf/tests: Define an OMB perf run template by @bharathv in #5141
    r/consensus: try updating leader commit index after aborting config c… by @mmaslankaprv in #5340
    r/recovery_stm: changed recovery stm append entries failure logging sev by @mmaslankaprv in #4561
    r/vote_stm: do not step down when replicating configuration failed by @mmaslankaprv in #4342
    readme: change note on beta releases (for RCs) by @ivotron in #3044
    rpk: add broker version info in 'redpanda admin broker ls' by @daisukebe in #4333
    rpk: change default_topic_partitions to -1 by @daisukebe in #5300
    serde serialization for topic_namespace by @felixguendling in #4821
    serde: added bytes support by @mmaslankaprv in #4841
    set default key rotation policy by @raresfirebolt in #5307
    tests/TopicRecovery: mark ok to fail by @NyaliaLui in #5005
    tests/offset_for_leader_epoch: increase start timeout by @mmaslankaprv in #4559
    tests: Add random action injector by @abhijat in #4404
    tests: Kstreams PageView improve logging by @NyaliaLui in #4805
    tests: added test validating creating and deleting topic many times by @mmaslankaprv in #4890
    tests: allow chaos error msg for cluster health test by @LenaAn in #4905
    tests: http imposter by @abhijat in #5239
    tests: scale mm2 consumer groups tests for CDT by @NyaliaLui in #4562
    tests: use rpk producer for even record distribution by @abhijat in #4928
    tools/metadata_viewer: Refactor metadata_viewer and add transaction metadata support by @bharathv in #5294
    tools: add shell script to run materialize tests by @NyaliaLui in #4740
    tools: added simple tool to map file path to the corresponding cloud storage path by @daisukebe in #4776
    vassert: prevent assert condition from influencing formatting by @mmaslankaprv in #5124

PRs that have 'none' or 'n/a' in the release notes section

    Accumulated miscellaneous changes by @dotnwat in #5505
    Add function to clear a container asynchronously by @ballard26 in #4860
    Align feature manager's logical versions with v22.1.5 by @rystsov in #5481
    Attempt to avoid repeated finds during housekeeping by @ballard26 in #4891
    Autogen request_api stubs by @graphcareful in #5037
    Bump seastar version for OSS builds by @VladLazar in #5359
    CODEOWNERS: update rpk by @twmb in #4388
    Capture redpanda version and timestamp metadata in OMB runs by @bharathv in #5502
    Factor out rate testing util for quota testing, etc. by @ajfabbri in #5201
    Fix for failure in nodes decommissioning test by @mmaslankaprv in #5341
    Fix logging in group_metadata_migration by @VadimPlh in #4902
    Fixed: find tran coordinator was not ACL verified by @dlex in #5370
    Generate correct stubs for tags defined within kafka schemas by @graphcareful in #5017
    Increase default node start timeout in tests by @rystsov in #4705
    Move feature_table outside from controller by @VadimPlh in #5220
    Post announcements to community Slack channels on releases by @nk-87 in #4511
    Post announcements to internal Slack channels on releases by @nk-87 in #4500
    Prepare for clang++-14 by @BenPope in #4759
    Rate limit leadership transfers by @ballard26 in #5159
    Reduce raft_availability ducktape execution time by @rystsov in #4598
    Remove shard_assignment dead code by @ballard26 in #4935
    Revert "Revert "k8s: remove produce-consume test"" by @ivotron in #4344
    Speed up leader balancer by removing unneeded waits by @ballard26 in #5135
    Test tweaks by @jcsp in #4763
    Tests: fix upgrade_test by @ztlpn in #5440
    Update seastar part1 by @BenPope in #5002
    Update seastar v22.2.x by @BenPope in #5083
    Use explicit adl<> encoding for RPC types by @dotnwat in #5029
    accept out_of_bounds exception for corrupt data by @felixguendling in #5306
    admin/build: introduce cluster config schema util by @bharathv in #5045
    admin: report maintenance mode status for brokers by @dotnwat in #4319
    archival_metadata_stm: return error code instead of throwing by @ztlpn in #4691
    build: Update seastar to avoid deprecated coroutine warning in clang-14 by @BenPope in #4908
    chore: fix warning backgrounding futures by @dotnwat in #4858
    chore: miscellaneous improvements and fixes by @dotnwat in #4859
    cloud_storage: Add SI space leak test by @Lazin in #4476
    cloud_storage: retry gracefully on EPIPE errors by @jcsp in #4392
    cloud_storage: use SAX parsing for partition manifest by @LenaAn in #4957
    cluster: add configs for read_replica to topic properties by @LenaAn in #5082
    cluster: add serde support for cluster::configuration_update_request by @dotnwat in #5237
    cluster: add serde support for transaction gateway service rpc types by @dotnwat in #5071
    cluster: add support for serde to metadata and id allocator service types by @dotnwat in #5053
    cluster: clean up config error logging by @jcsp in #4942
    cluster: fix disabling leader balancer at runtime by @jcsp in #4545
    cluster: fix reactor stalls during shutdown by @jcsp in #5151
    cluster: swallog+log unexpected exceptions in report_metrics by @jcsp in #4876
    cluster: use consistent chrono::duration type by @dotnwat in #5094
    cluster: use remote revision id for read replicas by @LenaAn in #5175
    cluster_features_test: use RedpandaInstaller by @andrwng in #5487
    compile with {fmt} v8 by @tchaikov in #4260
    config: adjust visibility of enable_rack_awareness by @jcsp in #4468
    config: fix name+handling of topic_partitions_reserve_shard0 by @jcsp in #5335
    config: redact secrets on bootstrap logging by @rystsov in #4654
    config: reduce min allowed storage_min_free_bytes by @ajfabbri in #5317
    controller/probe: Reduce lifecycle by @BenPope in #5470
    controller: add serde support for controller rpc messages by @dotnwat in #5281
    controller: add serde support for rpc messages by @dotnwat in #5250
    controller: add serde support to controller rpc message types by @dotnwat in #5301
    controller: add serde support to controller rpc types by @dotnwat in #5336
    core: fix various warnings in tests by @dotnwat in #4929
    ducktape: add consumer group consumer to franz_go_verifiable_test by @ztlpn in #4713
    ducktape: support TLS-enabled ducktape tests by @dotnwat in #4505
    ducky: add si test with blocked s3 by @LenaAn in #4478
    ducky: fix test_deletion_stops_move ci-failure by @rystsov in #5204
    ducky: improve logging by @rystsov in #4704
    fix various typos by @andrwng in #4916
    gha/rpk: split darwin/linux rpk artifact uploading by @ivotron in #5265
    gha: clean up error handling for /backport by @andrewhsu in #4334
    gitignore: add .task/ folder by @ivotron in #5283
    kafka/probe: seperate internal and public metrics by @NyaliaLui in #5377
    kafka/protocol: use default generated operator==() by @tchaikov in #5138
    kafka/server: Fix operator<< for offset_metadata by @VadimPlh in #4927
    kafka/server: Prefer ss::bool to bool for is_flex by @graphcareful in #5215
    kafka/server: run activate_feature in background by @VadimPlh in #4474
    kafka: fix replication factor when creating __consumer_offsets by @jcsp in #4454
    kafka: fix undefined behavior in consumer group topic migration by @dotnwat in #4487
    kafka: merge remote.readreplica and bucket by @LenaAn in #5455
    metrics: Move disk space metrics to /public_metrics by @BenPope in #5547
    net: Remove security::tls::principal_mapper by @BenPope in #5493
    non-functional changes spotted in review post-merge by @andrwng in #4938
    pandaproxy: Invert error_code dependencies by @BenPope in #4722
    raft: add serde support for append entries and heartbeat messages by @dotnwat in #5465
    read replica: use remote partition count by @LenaAn in #5228
    read_replica: Download manifest for read replica by @LenaAn in #5048
    readme: change domain name in docker pull URL by @ivotron in #4668
    reflection/type_traits: use is_specialization_of<> template helper by @tchaikov in #5511
    rm_stm: make recovery from memory reset faster by @rystsov in #5027
    rpc: adaptive rpc message encoding by @dotnwat in #4963
    rpc: fix assertion after parsing failure by @dotnwat in #4355
    rpc: graceful handling of unsupported methods and transport versions by @dotnwat in #4853
    rpc: remove demos by @dotnwat in #5352
    rpc: reset transport version via reconnect_transport by @dotnwat in #5520
    rpk topic consume patch by @twmb in #4583
    rpk tune ducktape tests by @r-vasquez in #5407
    rpk-operator refactor: allow cancelling admin API calls by propagating context by @nicolaferraro in #5067
    rpk: add redpanda.aggregate_metrics to cfg struct by @r-vasquez in #5513
    rpk: backward compatibility corrections by @r-vasquez in #5449
    rpk: create params Write method. by @r-vasquez in #4954
    rpk: fix panic on cluster config export by @ustadhm in #5421
    rpk: improve configuration update message by @dotnwat in #4512
    rpk: improve wording in topic create: replication factor must be odd by @r-vasquez in #4786
    rpk: introduce basic weak types for custom unmarshaling by @r-vasquez in #4894
    rpk: make redpanda_checkers run in order by @r-vasquez in #5477
    rpk: remove manager.FindOrGenerate from commands by @r-vasquez in #4832
    rpk: standardize command and flag output by @r-vasquez in #5447
    rptest: Add topic recovery end to end test by @Lazin in #4801
    rptest: Disable test_missing_segment by @Lazin in #4856
    rptest: Disable two flaky topic recovery tests by @Lazin in #5070
    security: don't log potentially sensitive messages by @andrwng in #4955
    serde: Support absl::flat_hash_map and absl::btree_set by @bharathv in #5431
    slash commands: Fix milestone auto mode bugs by @gousteris in #4296
    storage: fix intermittent failure in disposing_in_use_reader by @jcsp in #5144
    storage: fix test partition_size_while_cleanup by @jcsp in #5244
    test/resourcel_limit: use less segment_fallocation by @gousteris in #4750
    test: create directory for certs before copying by @dotnwat in #4569
    test: fix path to sarama produce_test binary by @dotnwat in #4357
    test: make clean_node safer against shutdown/kill race by @jcsp in #5303
    test: make docker-compose compatible with v2 by @dotnwat in #4516
    test: miscellaneous fixes by @dotnwat in #5001
    tests/rpk_producer: acks arg is an int, not a bool by @ajfabbri in #4593
    tests: adjusts assertion size for deleted segment by @abhijat in #4622
    tests: copy HttpServer script to node during test, add inject_remote_script helper by @jcsp in #4395
    tests: disable leader balancer in cluster_health_overview_test by @jcsp in #5254
    tests: disable topic auto creation by default by @jcsp in #4710
    tests: enable running shadow indexing/archival tests on clustered ducktape by @jcsp in #4386
    tests: ensure wait_until_result progresses by @abhijat in #4361
    tests: extend CHAOS_LOG_ALLOW_LIST & RESTART_LOG_ALLOW_LIST by @jcsp in #4688
    tests: extend log allow list for kgo-verifier tests by @jcsp in #4682
    tests: extend trim_results.py to reduce tarball size by @jcsp in #5287
    tests: fix BadLogLines for "No such file" messages by @jcsp in #4753
    tests: fix assertion in cluster_features_test by @jcsp in #4406
    tests: fix log allow list for topic recovery test by @jcsp in #4523
    tests: fix nodes_decommissioning_test by @jcsp in #4681
    tests: fix rare failure in test_old_node_join by @jcsp in #5101
    tests: fix test_changing_topic_retention_with_restart by @jcsp in #5331
    tests: handle Request errors in Admin.cluster_maintenance status by @jcsp in #4625
    tests: improve LogLevelTest to not rely on initial level=trace by @jcsp in #4393
    tests: log details on failure to stop redpanda by @jcsp in #5343
    tests: make FeaturesMultiNodeTest.test_upgrade wait for health tick by @jcsp in #5274
    tests: make test_si_cache_space_leak work on symlinked data dir by @jcsp in #4997
    tests: mark ok_to_fail test_node_storage_metrics by @jcsp in #4335
    tests: mitigate MaintenanceTest failures by @jcsp in #5409
    tests: move test executables out of /tmp by @jcsp in #4351
    tests: prepare ResourceSettings for use on EC2 nodes, create high scale mode for ManyPartitionsTest by @jcsp in #4376
    tests: quieten exceptions during redpanda startup, fix a bug in mirrormaker test teardown by @jcsp in #4343
    tests: re-use installs in upgrade tests by @andrwng in #5459
    tests: reduce partition count in debug mode for MultiRestartTest by @jcsp in #4372
    tests: refine OMB tests by @jcsp in #4707
    tests: reinstate node_metrics_test by @jcsp in #5047
    tests: retry describe_groups on leadership instability by @ballard26 in #4768
    tests: retry run_npm_cmd on network errors by @jcsp in #4641
    tests: segregate resource-intensive tests in scale_tests/ by @jcsp in #4683
    tests: segregate scale tests by @jcsp in #4529
    tests: shrink default ResourceSettings to 2 core + 2GB RAM by @jcsp in #4365
    tests: tolerate rpc errors in TopicDeleteStressTest by @jcsp in #4367
    tests: update test_suite_ec2 by @jcsp in #4350
    tests: use RedpandaService.start in RackAwareReplicaPlacementTest by @jcsp in #4405
    tools: update metadata viewer for features command by @jcsp in #5286
    treewide: Don't depend on ss::condition_variable being assignable by @BenPope in #5003
    treewide: Reduce include dependencies by @BenPope in #5104
    treewide: Refactor operator<< as hidden friend by @BenPope in #4760
    update more copyright headers to use Redpanda Data by @andrewhsu in #4354
    v/rpc: use std::constructible_from when appropriate by @tchaikov in #5145
    vim: add coc-settings.json to src/v by @andrwng in #4808

PRs that have a single line in the release notes section

    #3279 * Implement KIP-447. In EOS transaction producer can work with consumer groups. by @VadimPlh in #5162
    #4366 * improve compatibility with franz-go by @rystsov in #4669
    #4433 * rpk bugfix: running rpk wasm generate path/to/dir on an existent directory made rpk falsely claim that main.js. main.test.js, package.json, and webpack.js existed. by @r-vasquez in #4580
    #4911 * rpk now redacts all unknown field values when using rpk debug bundle by @r-vasquez in #4915
    #5186 * rpk bugfix: Now rpk exits with code 1 when the rpk redpanda tune command either fails to run, or it has a tuner enabled but it's not supported by @r-vasquez in #5295
    #5355 * Add a workaround to bypass a java kafka client's consistency issue by @rystsov in #5356
    #5391 * fix handling of shadow indexing flags by @Lazin in #5392
    #5494 * The old default redpanda.yaml incorrectly specified the pandaproxy and schema_registry, and rpk had a bug that always added those sections by default. rpk will no longer add those sections no matter what, but the yaml file also now includes the sections properly by default. Using a new rpk on an old default yaml will no longer add the missing pandaproxy and schema_registry sections. by @r-vasquez in #5503
    #5521. * rpk: fix cluster health never showing leaderless partitions by @travisdowns in #5523
        A new --version flag is added to the redpanda binary. When supplied, the program will print the version and exit immediately. by @andrwng in #5123
        Enable idempotency by default. by @RafalKorepta in #4627
        You can now manage your cluster licenses with rpk cluster license by @r-vasquez in #5223
        fixes aborted reads violation by @rystsov in #5025
        rpk: add rpk redpanda tune list command to check for available tuners. by @r-vasquez in #5298
    Fixes: * rpk: more descriptive logs for tuner checkers in rpk redpanda tune and rpk redpanda start commands. by @r-vasquez in #5462
    This PR is a response to #4394 (comment) by @NyaliaLui in #4491
    a * rpk: Fix a bug that caused rpk topic add-partitions to panic when an unfeasible number of partitions were passed by @r-vasquez in #5369
    a * rpk: fixed bug in IPv6 parsing that previously caused IPv6 hosts to be doubly wrapped in brackets by @r-vasquez in #4777

PRs that have multiple lines in the release notes section

    /backport on PR: remove quotes when cherry-picking by @gousteris in #4989
    Add hooks for rolling restarts and upgrades by @nicolaferraro in #4125
    Add test for full disk threshold config, leave default as-is by @ajfabbri in #5216
    Added rpk cluster health command by @mmaslankaprv in #4295
    Added serde serialization support to cluster types by @mmaslankaprv in #5033
    Added handling members with group.instance.id set i.e. static members by @mmaslankaprv in #4684
    Added serde support for basic common types by @mmaslankaprv in #5049
    Aggregate metrics to reduce cardinality by @VladLazar in #5166
    Allow having multiple TLS kafka listeners by @alenkacz in #4718
    Break ties randomly in partition allocator by @travisdowns in #4974
    Catch unhandeled exceptions within net::server by @graphcareful in #5211
    Cleanups inspired by gcc by @tchaikov in #5304
    Compaction keyspaces by @mmaslankaprv in #5345
    Default partitions count for __consumer_offsets topic increased by @dlex in #5412
    Drop the CONCEPT macro by @tchaikov in #4919
    Fetch long poll fix by @mmaslankaprv in #4506
    Fix linearizable barrier by @mmaslankaprv in #4555
    Fix lost update consistency error by @rystsov in #5026
    Fix raft linearizable barrier by @mmaslankaprv in #4507
    Fixed dispatching update finished command after partition reconfiguration by @mmaslankaprv in #5357
    Fixed isolation availability issue by @mmaslankaprv in #4368
    Fixed serialization of group tombstones by @mmaslankaprv in #4901
    Handle aborted transactions in shadow indexing by @Lazin in #5381
    Implemented nodes recommissioning by @mmaslankaprv in #5323
    Implemented partition movement interruption by @mmaslankaprv in #5158
    Improve idempotency latency by introducing rm_stm pipelining by @rystsov in #5157
    Introduce license checks for redpanda by @graphcareful in #5190
    Made recovery throttle runtime configurable by @mmaslankaprv in #5108
    Minimal primary metrics endpoint by @VladLazar in #5165
    Minor improvement, switch to to_array by @theidexisted in #4741
    Node operations fuzzy improvements by @mmaslankaprv in #5360
    Operator: add support for downscaling by @nicolaferraro in #5019
    Optimized partition metadata lookup by @mmaslankaprv in #4443
    Partition balancer backend by @ztlpn in #5371
    Partition movement interruption APIs by @mmaslankaprv in #5334
    Partitions subcommand by @lacer-utkarsh in #4425
    Pin Go dependency versions for test Dockerfile by @nk-87 in #4553
    Refactor TLS certificates handling in k8s operator by @alenkacz in #4540
    Reject writes as needed to avoid full disks (v1) by @ajfabbri in #4803
    Release SMP service group before 2nd stage by @travisdowns in #4980
    Remove viper and mapstructure from rpk by @r-vasquez in #5061
    Support for flexible requests by @graphcareful in #4513
    Two phase group processing follow up by @mmaslankaprv in #4775
    Two stage processing in groups by @mmaslankaprv in #4579
    Update external connectivity docs to get a valid VERSION environment variable by @vuldin in #4988
    Use correct fn to move offset translator state by @ztlpn in #4493
    Using serde in redpanda/controller/0 partition by @mmaslankaprv in #4889
    admin: reject maintenance mode req on 1 node cluster by @jcsp in #4921
    archival: Truncate archival metadata snapshot by @Lazin in #5160
    build: drop unused set_option.cmake by @tchaikov in #5236
    c/dissemination: do not query topic metadata for ntp by @mmaslankaprv in #4389
    c/md_dissemination: do not capture unused variable by @tchaikov in #4715
    cloud_storage: Improve SI cache by @Lazin in #5044
    cloud_storage: add new error counting metrics by @NyaliaLui in #5367
    cloud_storage: downgrade another log error to a warning by @jcsp in #4408
    cloud_storage: support IAM roles in redpanda by @abhijat in #5309
    cloud_storage: treat S3 InternalError as a backoff by @jcsp in #4706
    cluster, kafka, raft: Logging refinement by @jcsp in #4977
    cluster,kafka: auto-populate cluster_id and prefix it with "redpanda." by @jcsp in #4457
    cluster: bootstrap user creation during cluster creation by @jcsp in #5231
    cluster: check abort source in reconcile_ntp loop by @jcsp in #4720
    cluster: downgrade per-partition messages to DEBUG by @jcsp in #4951
    cluster: drop redundant set_status config updates by @jcsp in #4924
    cluster: make shards-per-core parameters configurable at runtime by @jcsp in #5205
    cmake_test: catch exceptional future ignored backtraces by @dotnwat in #4855
    dashboard: remove unused dashboard code by @jcsp in #4542
    docs(/src/.md): Readability improvements by @ryanrussell in #5396
    fix build failure with autoconf 2.70+ by @andrwng in #4774
    k/protocol: move kafka_request_schemata into v::kafka_protocol by @tchaikov in #5139
    k/replicated_partition: do not validate fetch offset when stopping by @mmaslankaprv in #5066
    k8s,docs: bump cert-manager to v1.4.4 by @sbocinec in #5096
    k8s: fix lint warnings and disabling gomnd by @r-vasquez in #4396
    kafka/group: add new consumer group metrics by @NyaliaLui in #5349
    kafka/server: add missing placeholder in format string. by @balusch in #5320
    kafka: add read replica fields to create topic request by @LenaAn in #5100
    kafka: introduce separate TCP buffer sizes for kafka vs. internal RPC by @jcsp in #5099
    kafka: mTLS principal mapping by @BenPope in #4501
    kafka: validate user provided create_topic configuration literals by @VladLazar in #4953
    misc fixes identified when compiling the tree with clang-15 + fmt-v8 by @tchaikov in #4306
    net/kafka/rpc/cluster: Improved logging by @jcsp in #5097
    net: add new rpc metrics by @NyaliaLui in #5411
    pandaproxy: add metrics for error count to /public_metrics by @BenPope in #5457
    raft: add serde support for raft message types by @dotnwat in #5353
    read replica: always serve cloud data for read replicas by @LenaAn in #5242
    redpanda: Sort config options before logging by @BenPope in #4604
    redpanda: per listener authentication for kafka by @BenPope in #5292
    rpc: add method_id accessors for generated code by @andrwng in #4893
    rpc: enable setting a limit on the seastar/userspace receive buffer size by @jcsp in #2275
    rpk patches before v22.1.1 by @twmb in #4570
    rpk redpanda admin command for MacOS by @lacer-utkarsh in #4291
    rpk redpanda config set: improve setting arrays by @twmb in #5522
    rpk/config: Add ServerTLS:Other by @BenPope in #4603
    rpk/k8s: enable gofumpt as default go formatter by @r-vasquez in #4653
    rpk: Add golangci-lint by @r-vasquez in #4387
    rpk: Support listener authN by @BenPope in #5482
    rpk: add debug logdirs describe by @twmb in #4979
    rpk: add cluster maintenance status and enable barrier by @dotnwat in #4390
    rpk: add metadata request in client creation to control sasl errors by @r-vasquez in #4727
    rpk: add summary flag to group describe by @r-vasquez in #4799
    rpk: allow write_disk_cache to fail in rpk tune by @r-vasquez in #5348
    rpk: avoid duplicates in container port pool by @r-vasquez in #4721
    rpk: avoid transmuting CLI args without equal sign by @r-vasquez in #5397
    rpk: change Invalid Partitions err message in topic creation by @r-vasquez in #4662
    rpk: change error in cluster health flag description by @r-vasquez in #4703
    rpk: change exit status for topic command failures by @r-vasquez in #4592
    rpk: change host flag help text for admin config print by @r-vasquez in #4617
    rpk: changes in cluster config: by @r-vasquez in #4636
    rpk: enable linters Part 2. by @r-vasquez in #4412
    rpk: fix build for windows OS by @r-vasquez in #5260
    rpk: fix data race in container purge and stop by @r-vasquez in #4586
    rpk: generate pool of available ports for container start command by @r-vasquez in #4565
    rpk: include hint to set env variable when rpk container starts by @r-vasquez in #4800
    rpk: remove base58 encoding to node.uuid to avoid confusion by @r-vasquez in #4514
    rpk: remove readAsJson from config manager by @r-vasquez in #4909
    rpk: rename httpError for httpResponseError by @r-vasquez in #4912
    rpk: support hostnames in config bootstrapping by @r-vasquez in #4770
    rpk: update feedback URL by @sbocinec in #5080
    rptest: Add new test for shadow indexing by @Lazin in #3532
    s3: Improve error handling by @Lazin in #4749
    schema_registry: Support GET /mode by @BenPope in #5098
    serde: use concepts instead of type traits implemented using SFINAE by @tchaikov in #5288
    storage: Fix dynamic reconfiguration of log segment size by @bharathv in #5106
    storage: do not hold open FDs for all segments+indices by @jcsp in #4022
    storage: don't log errors during recovery by @jcsp in #4969
    storage: explicitly mark clean segments in kvstore on clean shutdown by @jcsp in #5127
    storage: introduce storage_resources for controlling resource use at scale by @jcsp in #5150
    test infrastructure to support upgrade testing by @andrwng in #5155
    tests: Deflake test_migrating_consume_offsets by @bharathv in #5074

