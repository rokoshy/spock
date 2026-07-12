use strict;
use warnings;
use Test::More;
use lib '.';
use lib 't';
use SpockTest qw(
    create_cluster destroy_cluster
    system_or_bail system_maybe command_ok
    get_test_config scalar_query psql_or_bail
    wait_for_sub_status
);
use Time::HiRes qw(sleep time);

# Verify that feedback sent while a remote transaction is still open reports
# receipt only.  The publisher must not advance confirmed_flush_lsn until the
# subscriber commits the complete transaction locally.

sub qport {
    my ($pg_bin, $host, $port, $dbname, $user, $sql) = @_;
    my $out = `$pg_bin/psql -X -h $host -p $port -d $dbname -U $user -t -c "$sql" 2>/dev/null`;
    $out //= '';
    $out =~ s/^\s+|\s+$//g;
    return $out;
}

sub wait_until {
    my ($timeout, $poll, $condition) = @_;
    my $deadline = time() + $timeout;
    while (time() < $deadline) {
        return 1 if $condition->();
        sleep($poll);
    }
    return 0;
}

create_cluster(2, 'Create 2-node cluster for open-transaction feedback test');

my $config      = get_test_config();
my $node_ports  = $config->{node_ports};
my $node_dirs   = $config->{node_datadirs};
my $host        = $config->{host};
my $dbname      = $config->{db_name};
my $db_user     = $config->{db_user};
my $db_password = $config->{db_password};
my $pg_bin      = $config->{pg_bin};

my $provider_port   = $node_ports->[0];
my $subscriber_port = $node_ports->[1];
my $provider_dsn =
    "host=$host dbname=$dbname port=$provider_port " .
    "user=$db_user password=$db_password";

# Make the unsafe path deterministic.  The unpatched worker force-sends
# feedback before every XLogData message after the first one.
psql_or_bail(2, "ALTER SYSTEM SET spock.feedback_frequency = '1'");
psql_or_bail(2, "SELECT pg_reload_conf()");
is(scalar_query(2, "SHOW spock.feedback_frequency"), '1',
    'subscriber sends frequent feedback during the regression');

# Match the supported Ultra-HA topology before creating the logical
# subscription: the logical subscriber also has a synchronous physical
# standby.  Starting the publisher slot only after this gate is active keeps
# the initial confirmation boundary failover-safe as well.
psql_or_bail(2,
    "SELECT pg_create_physical_replication_slot('open_xact_sync_slot')");

my $standby_port = $subscriber_port + 20;
my $standby_datadir = '/tmp/tmp_spock_open_xact_standby';
my $standby_logdir = "$standby_datadir/pg_log";
system("rm -rf $standby_datadir 2>/dev/null");
system_or_bail("$pg_bin/pg_basebackup",
    '-D', $standby_datadir,
    '-h', $host, '-p', $subscriber_port, '-U', $db_user,
    '-X', 'stream', '-R');
system_or_bail('mkdir', '-p', $standby_logdir);
{
    open(my $conf, '>>', "$standby_datadir/postgresql.conf")
        or die "Cannot open standby postgresql.conf: $!";
    print $conf "\nport = $standby_port\n";
    print $conf "hot_standby = on\n";
    print $conf "hot_standby_feedback = on\n";
    print $conf "primary_slot_name = 'open_xact_sync_slot'\n";
    print $conf "log_directory = '$standby_logdir'\n";
    print $conf "log_filename = 'standby.log'\n";
    close($conf);
}
system_or_bail("$pg_bin/pg_ctl", 'start',
    '-D', $standby_datadir, '-l', "$standby_datadir/startup.log", '-w');
command_ok(["$pg_bin/pg_isready", '-h', $host, '-p', $standby_port],
    'subscriber physical standby accepts connections');
is(qport($pg_bin, $host, $standby_port, $dbname, $db_user,
        'SELECT pg_is_in_recovery()'),
    't', 'subscriber physical standby is in recovery');

ok(wait_until(30, 0.2, sub {
        scalar_query(2,
            "SELECT count(*) FROM pg_stat_replication " .
            "WHERE application_name = 'walreceiver'") eq '1';
    }), 'subscriber physical standby is streaming');
psql_or_bail(2,
    "ALTER SYSTEM SET synchronous_standby_names = 'walreceiver'");
psql_or_bail(2, "ALTER SYSTEM SET synchronous_commit = 'on'");
psql_or_bail(2, "SELECT pg_reload_conf()");
ok(wait_until(10, 0.2, sub {
        scalar_query(2,
            "SELECT count(*) FROM pg_stat_replication " .
            "WHERE application_name = 'walreceiver' " .
            "AND sync_state = 'sync' AND flush_lsn IS NOT NULL") eq '1';
    }), 'subscriber physical standby is selected as synchronous and flushing');

psql_or_bail(1, q{
    CREATE TABLE test_open_xact_feedback
    (
        id INTEGER PRIMARY KEY,
        payload TEXT NOT NULL
    )
});
psql_or_bail(2, q{
    CREATE TABLE test_open_xact_feedback
    (
        id INTEGER PRIMARY KEY,
        payload TEXT NOT NULL
    );
});

psql_or_bail(2,
    "SELECT spock.sub_create('sub_open_xact_feedback', '$provider_dsn', " .
    "ARRAY['default', 'default_insert_only'], false, false)");

ok(wait_for_sub_status(2, 'sub_open_xact_feedback', 'replicating', 30),
    'subscription reaches replicating state');

my $slot_name = '';
for (1 .. 30) {
    $slot_name = scalar_query(1,
        "SELECT slot_name FROM pg_replication_slots " .
        "WHERE slot_type = 'logical' AND active LIMIT 1");
    last if defined $slot_name && $slot_name ne '';
    sleep(0.2);
}
ok(defined $slot_name && $slot_name ne '',
    "provider logical slot is active: $slot_name");

# Establish a committed feedback boundary before opening the large
# transaction.
psql_or_bail(1,
    "INSERT INTO test_open_xact_feedback VALUES (0, 'baseline')");

my $baseline_replicated = 0;
for (1 .. 100) {
    $baseline_replicated = scalar_query(2,
        "SELECT count(*) FROM test_open_xact_feedback WHERE id = 0");
    last if defined $baseline_replicated && $baseline_replicated eq '1';
    sleep(0.2);
}
is($baseline_replicated, '1', 'baseline row is durably applied');

psql_or_bail(2, q{
    CREATE SEQUENCE test_open_xact_feedback_entered_seq;

    CREATE FUNCTION test_open_xact_feedback_pause_once() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF nextval('test_open_xact_feedback_entered_seq') = 1 THEN
            PERFORM pg_sleep(15);
        END IF;
        RETURN NEW;
    END
    $$;

    CREATE TRIGGER test_open_xact_feedback_pause_once_trigger
        BEFORE INSERT ON test_open_xact_feedback
        FOR EACH ROW
        EXECUTE FUNCTION test_open_xact_feedback_pause_once();

    ALTER TABLE test_open_xact_feedback
        ENABLE REPLICA TRIGGER test_open_xact_feedback_pause_once_trigger;
});

my $durable_origin = scalar_query(2, q{
    SELECT remote_lsn
      FROM pg_replication_origin_status
     WHERE external_id = '} . $slot_name . q{'
});
like($durable_origin, qr{^[0-9A-F]+/[0-9A-F]+$},
    "captured subscriber durable origin $durable_origin");

my $confirmed_before = scalar_query(1,
    "SELECT confirmed_flush_lsn FROM pg_replication_slots " .
    "WHERE slot_name = '$slot_name'");
like($confirmed_before, qr{^[0-9A-F]+/[0-9A-F]+$},
    "captured publisher confirmed position $confirmed_before");
is(scalar_query(1,
        "SELECT ('$confirmed_before'::pg_lsn <= " .
        "'$durable_origin'::pg_lsn)::int"),
    '1', 'test starts from a safe publisher confirmation boundary');

# About 16 MiB generates more XLogData messages than the normal feedback
# threshold and prevents the transaction COMMIT from being mistaken for the
# first-row pause point.
psql_or_bail(1, q{
    INSERT INTO test_open_xact_feedback
    SELECT i,
           (SELECT string_agg(md5((i * 1024 + j)::text), '')
              FROM generate_series(1, 1024) AS j)
      FROM generate_series(1, 512) AS i
});

my $apply_entered = 0;
for (1 .. 100) {
    $apply_entered = scalar_query(2,
        "SELECT is_called::int " .
        "FROM test_open_xact_feedback_entered_seq");
    last if defined $apply_entered && $apply_entered eq '1';
    sleep(0.1);
}
is($apply_entered, '1',
    'subscriber is paused inside the first row of the remote transaction');

is(scalar_query(2,
        "SELECT count(*) FROM test_open_xact_feedback WHERE id > 0"),
    '0', 'open remote transaction has no locally visible rows');

# Prove that feedback reached the provider while the transaction is still
# blocked.  Receipt may advance, but flush/apply must remain at the last
# durable subscriber origin until the complete transaction commits.
my $receipt_advanced = 0;
for (1 .. 50) {
    $receipt_advanced = scalar_query(1,
        "SELECT COALESCE((write_lsn > '$durable_origin'::pg_lsn)::int, 0) " .
        "FROM pg_stat_replication WHERE application_name = '$slot_name'");
    last if defined $receipt_advanced && $receipt_advanced eq '1';
    sleep(0.1);
}
is($receipt_advanced, '1',
    'provider received a status reply while the transaction is open');

my $confirmed_during = scalar_query(1,
    "SELECT confirmed_flush_lsn FROM pg_replication_slots " .
    "WHERE slot_name = '$slot_name'");
my $unsafe_ahead = scalar_query(1,
    "SELECT ('$confirmed_during'::pg_lsn > '$durable_origin'::pg_lsn)::int");

diag("durable origin before transaction: $durable_origin");
diag("publisher confirmed before:       $confirmed_before");
diag("publisher confirmed while open:   $confirmed_during");
is($unsafe_ahead, '0',
    'publisher does not confirm WAL beyond the subscriber durable origin');

my $row_count = 0;
for (1 .. 120) {
    $row_count = scalar_query(2,
        "SELECT count(*) FROM test_open_xact_feedback WHERE id > 0");
    last if defined $row_count && $row_count eq '512';
    sleep(0.5);
}
is($row_count, '512', 'all 512 rows commit on the subscriber');

my $confirmed_after = $confirmed_during;
for (1 .. 60) {
    $confirmed_after = scalar_query(1,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots " .
        "WHERE slot_name = '$slot_name'");
    last if scalar_query(1,
        "SELECT ('$confirmed_after'::pg_lsn > " .
        "'$confirmed_during'::pg_lsn)::int") eq '1';
    sleep(0.5);
}
is(scalar_query(1,
        "SELECT ('$confirmed_after'::pg_lsn > " .
        "'$confirmed_during'::pg_lsn)::int"),
    '1', 'publisher confirmation advances after subscriber commit');

# Two rapid commits must not leave the last hold pinned at the preceding
# transaction's remote LSN.  No later replicated transaction is sent before
# checking the second boundary.
psql_or_bail(1,
    "INSERT INTO test_open_xact_feedback VALUES (1001, 'rapid_commit_1')");
psql_or_bail(1,
    "INSERT INTO test_open_xact_feedback VALUES (1002, 'rapid_commit_2')");
ok(wait_until(30, 0.2, sub {
        scalar_query(2,
            "SELECT count(*) FROM test_open_xact_feedback " .
            "WHERE id IN (1001, 1002)") eq '2';
    }), 'two rapid commits are applied without a following transaction');

my $origin_after_two = scalar_query(2, q{
    SELECT remote_lsn
      FROM pg_replication_origin_status
     WHERE external_id = '} . $slot_name . q{'
});
my $two_commit_confirmed = 0;
ok(wait_until(30, 0.2, sub {
        $two_commit_confirmed = scalar_query(1,
            "SELECT (confirmed_flush_lsn >= " .
            "'$origin_after_two'::pg_lsn)::int " .
            "FROM pg_replication_slots WHERE slot_name = '$slot_name'");
        defined $two_commit_confirmed && $two_commit_confirmed eq '1';
    }), 'publisher confirms the second rapid commit without a third commit');

# Transaction-safe feedback deliberately does not flush-ack keepalive-only
# progress because the wire message cannot distinguish an excluded transaction
# from a relevant transaction whose BEGIN has not arrived yet.  Verify that
# receipt still advances and that the next replicated probe commit releases all
# earlier WAL, bounding retention in installations that run a periodic probe.
psql_or_bail(1, q{
    SET spock.enable_ddl_replication = off;

    CREATE TABLE test_filtered_feedback_wal
    (
        id INTEGER PRIMARY KEY,
        payload TEXT NOT NULL
    );

    DO $$
    DECLARE
        set_record RECORD;
    BEGIN
        FOR set_record IN
            SELECT replication_set.set_name
              FROM spock.replication_set_table
              JOIN spock.replication_set
                ON replication_set.set_id = replication_set_table.set_id
             WHERE replication_set_table.set_reloid =
                   'test_filtered_feedback_wal'::regclass
        LOOP
            PERFORM spock.repset_remove_table(
                set_record.set_name,
                'test_filtered_feedback_wal'::regclass);
        END LOOP;
    END
    $$;
});
psql_or_bail(2, q{
    CREATE TABLE IF NOT EXISTS test_filtered_feedback_wal
    (
        id INTEGER PRIMARY KEY,
        payload TEXT NOT NULL
    )
});
is(scalar_query(1, q{
        SELECT count(*)
          FROM spock.replication_set_table
         WHERE set_reloid = 'test_filtered_feedback_wal'::regclass
    }), '0', 'filtered WAL table is absent from every replication set');

psql_or_bail(1, "ALTER SYSTEM SET wal_sender_timeout = '4s'");
psql_or_bail(1, "SELECT pg_reload_conf()");

my $confirmed_before_filtered = scalar_query(1,
    "SELECT confirmed_flush_lsn FROM pg_replication_slots " .
    "WHERE slot_name = '$slot_name'");
my $filtered_wal_start = scalar_query(1, "SELECT pg_current_wal_lsn()");
psql_or_bail(1, q{
    INSERT INTO test_filtered_feedback_wal
    SELECT i, repeat(md5(i::text), 128)
      FROM generate_series(1, 256) AS i
});
my $filtered_wal_end = scalar_query(1, "SELECT pg_current_wal_lsn()");

my $filtered_receipt_advanced = 0;
for (1 .. 100) {
    $filtered_receipt_advanced = scalar_query(1,
        "SELECT COALESCE((write_lsn >= '$filtered_wal_end'::pg_lsn)::int, 0) " .
        "FROM pg_stat_replication WHERE application_name = '$slot_name'");
    last if defined $filtered_receipt_advanced &&
            $filtered_receipt_advanced eq '1';
    sleep(0.1);
}
is($filtered_receipt_advanced, '1',
    'keepalive acknowledges receipt of filtered WAL');

my $confirmed_after_filtered = scalar_query(1,
    "SELECT confirmed_flush_lsn FROM pg_replication_slots " .
    "WHERE slot_name = '$slot_name'");
diag("publisher confirmed before filtered WAL: $confirmed_before_filtered");
diag("filtered WAL start/end: $filtered_wal_start / $filtered_wal_end");
diag("publisher confirmed after filtered WAL:  $confirmed_after_filtered");
is(scalar_query(1,
        "SELECT ('$confirmed_after_filtered'::pg_lsn <= " .
        "'$filtered_wal_start'::pg_lsn)::int"),
    '1', 'filtered transaction is not falsely acknowledged as applied');

psql_or_bail(1,
    "INSERT INTO test_open_xact_feedback VALUES (1000, 'periodic_probe')");
my $probe_replicated = 0;
for (1 .. 100) {
    $probe_replicated = scalar_query(2,
        "SELECT count(*) FROM test_open_xact_feedback WHERE id = 1000");
    last if defined $probe_replicated && $probe_replicated eq '1';
    sleep(0.2);
}
is($probe_replicated, '1', 'periodic probe commit is applied');

my $confirmed_after_probe = $confirmed_after_filtered;
for (1 .. 100) {
    $confirmed_after_probe = scalar_query(1,
        "SELECT confirmed_flush_lsn FROM pg_replication_slots " .
        "WHERE slot_name = '$slot_name'");
    last if scalar_query(1,
        "SELECT ('$confirmed_after_probe'::pg_lsn > " .
        "'$filtered_wal_end'::pg_lsn)::int") eq '1';
    sleep(0.2);
}
is(scalar_query(1,
        "SELECT ('$confirmed_after_probe'::pg_lsn > " .
        "'$filtered_wal_end'::pg_lsn)::int"),
    '1', 'next replicated probe releases all preceding filtered WAL');

is(scalar_query(2,
        "SELECT sub_enabled FROM spock.subscription " .
        "WHERE sub_name = 'sub_open_xact_feedback'"),
    't', 'subscription remains enabled');

is(scalar_query(2, "SELECT count(*) FROM spock.exception_log"),
    '0', 'transaction-safe feedback produces no apply exception');

system_maybe("$pg_bin/psql", '-h', $host, '-p', $subscriber_port,
    '-U', $db_user, '-d', $dbname,
    '-c', "SELECT spock.sub_drop('sub_open_xact_feedback')");

system_maybe("$pg_bin/psql", '-h', $host, '-p', $subscriber_port,
    '-U', $db_user, '-d', $dbname,
    '-c', "ALTER SYSTEM RESET synchronous_standby_names");
system_maybe("$pg_bin/psql", '-h', $host, '-p', $subscriber_port,
    '-U', $db_user, '-d', $dbname,
    '-c', "ALTER SYSTEM RESET synchronous_commit");
system_maybe("$pg_bin/psql", '-h', $host, '-p', $subscriber_port,
    '-U', $db_user, '-d', $dbname,
    '-c', "SELECT pg_reload_conf()");
system_maybe("$pg_bin/pg_ctl", 'stop',
    '-D', $standby_datadir, '-m', 'immediate');
system("rm -rf $standby_datadir 2>/dev/null");

destroy_cluster('Destroy open-transaction feedback test cluster');

done_testing();
