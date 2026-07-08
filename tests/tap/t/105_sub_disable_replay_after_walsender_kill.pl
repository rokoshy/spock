use strict;
use warnings;
use Test::More;
use lib '.';
use SpockTest qw(
    create_cluster destroy_cluster
    system_maybe
    get_test_config scalar_query psql_or_bail
    wait_for_pg_ready wait_for_sub_status
);

# Reproduce a provider crash while the subscriber is inside a remote
# transaction.  The first replica-trigger invocation sleeps long enough for
# this test to SIGKILL the provider WAL sender.  The transaction is deliberately
# larger than normal socket buffers, so its COMMIT cannot already be queued at
# the subscriber while the first row is blocked.  PostgreSQL treats an
# unexpected WAL-sender SIGKILL as a child crash, terminates sibling backends,
# performs crash recovery, and then accepts the subscriber reconnect.

create_cluster(2, 'Create 2-node cluster for provider SIGKILL replay test');

my $config      = get_test_config();
my $node_ports  = $config->{node_ports};
my $host        = $config->{host};
my $dbname      = $config->{db_name};
my $db_user     = $config->{db_user};
my $db_password = $config->{db_password};
my $pg_bin      = $config->{pg_bin};
my $log_dir     = $config->{log_dir};
my $node_dirs   = $config->{node_datadirs};

my $p1 = $node_ports->[0];
my $p2 = $node_ports->[1];
my $conn_n1 = "host=$host dbname=$dbname port=$p1 user=$db_user password=$db_password";
my $pg_log_n1 = $log_dir =~ m{^/}
    ? "$log_dir/00${p1}.log"
    : "$node_dirs->[0]/$log_dir/00${p1}.log";
my $pg_log_n2 = $log_dir =~ m{^/}
    ? "$log_dir/00${p2}.log"
    : "$node_dirs->[1]/$log_dir/00${p2}.log";

psql_or_bail(2, "ALTER SYSTEM SET spock.exception_behaviour = 'sub_disable'");
psql_or_bail(2, "SELECT pg_reload_conf()");

psql_or_bail(1,
    "CREATE TABLE test_walsender_kill_replay (id INTEGER PRIMARY KEY, payload TEXT)");
psql_or_bail(2,
    "CREATE TABLE test_walsender_kill_replay (id INTEGER PRIMARY KEY, payload TEXT)");

psql_or_bail(2, q{
    CREATE SEQUENCE test_walsender_kill_entered_seq;
    CREATE FUNCTION test_walsender_kill_pause_once() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF nextval('test_walsender_kill_entered_seq') = 1 THEN
            PERFORM pg_sleep(15);
        END IF;
        RETURN NEW;
    END
    $$;
    CREATE TRIGGER test_walsender_kill_pause_once_trigger
        BEFORE INSERT ON test_walsender_kill_replay
        FOR EACH ROW EXECUTE FUNCTION test_walsender_kill_pause_once();
    ALTER TABLE test_walsender_kill_replay
        ENABLE REPLICA TRIGGER test_walsender_kill_pause_once_trigger;
});

psql_or_bail(2,
    "SELECT spock.sub_create('sub_n1_n2', '$conn_n1', " .
    "ARRAY['default', 'default_insert_only', 'ddl_sql'], false, false)");

ok(wait_for_sub_status(2, 'sub_n1_n2', 'replicating', 30),
    'sub_n1_n2 reaches replicating state');

my $provider_log_offset = -s $pg_log_n1 // 0;
my $subscriber_log_offset = -s $pg_log_n2 // 0;

# About 16 MiB of incompressible text keeps the WAL sender behind the blocked
# first-row trigger.  The source commit completes before logical decoding starts.
psql_or_bail(1, q{
    INSERT INTO test_walsender_kill_replay
    SELECT i,
           (SELECT string_agg(md5((i * 1024 + j)::text), '')
              FROM generate_series(1, 1024) AS j)
      FROM generate_series(1, 512) AS i
});

my $apply_entered = 0;
for (1 .. 100) {
    $apply_entered = scalar_query(2,
        "SELECT is_called::int FROM test_walsender_kill_entered_seq");
    last if defined $apply_entered && $apply_entered eq '1';
    select undef, undef, undef, 0.1;
}
is($apply_entered, '1', 'subscriber entered first-row replica trigger');

my $walsender_pid = scalar_query(1,
    "SELECT pid FROM pg_stat_replication WHERE state = 'streaming' LIMIT 1");
BAIL_OUT("no streaming WAL sender to SIGKILL")
    unless defined $walsender_pid && $walsender_pid =~ /^\d+$/;

my $signaled = kill 9, int($walsender_pid);
ok($signaled, "SIGKILL sent to provider WAL sender PID $walsender_pid");
BAIL_OUT("could not SIGKILL provider WAL sender PID $walsender_pid")
    unless $signaled;

ok(wait_for_pg_ready($host, $p1, $pg_bin, 60),
    'provider accepts connections after crash recovery');
ok(wait_for_sub_status(2, 'sub_n1_n2', 'replicating', 90),
    'subscriber reconnects after provider crash recovery');

my $row_count = 0;
for (1 .. 120) {
    $row_count = scalar_query(2,
        "SELECT count(*) FROM test_walsender_kill_replay");
    last if defined $row_count && $row_count eq '512';
    sleep(1);
}
is($row_count, '512',
    'aborted in-flight transaction is fully retransmitted and applied');

is(scalar_query(2,
    "SELECT sub_enabled FROM spock.subscription WHERE sub_name = 'sub_n1_n2'"),
    't', 'subscription remains enabled');

my $provider_log = '';
if (open(my $lf, '<', $pg_log_n1)) {
    seek($lf, $provider_log_offset, 0);
    local $/;
    $provider_log = <$lf> // '';
    close($lf);
}

my $subscriber_log = '';
if (open(my $lf, '<', $pg_log_n2)) {
    seek($lf, $subscriber_log_offset, 0);
    local $/;
    $subscriber_log = <$lf> // '';
    close($lf);
}

like($provider_log,
    qr/was terminated by signal 9|terminating any other active server processes/,
    'provider log proves WAL-sender SIGKILL triggered crash handling');
like($subscriber_log,
    qr/connection error during apply, exiting via rethrow/,
    'subscriber detected provider loss while applying remote transaction');
like($subscriber_log,
    qr/cleared transient exception state after provider connection loss/,
    'subscriber cleared the in-flight exception marker before restart');
unlike($subscriber_log,
    qr/exception handling had no exception\(s\)/,
    'retransmitted transaction does not enter empty exception replay');

system_maybe("$pg_bin/psql", '-h', $host, '-p', $p2,
    '-U', $db_user, '-d', $dbname,
    '-c', "SELECT spock.sub_drop('sub_n1_n2')");

destroy_cluster('Destroy provider SIGKILL replay test cluster');

done_testing();
