use strict;
use warnings;
use Test::More;
use lib '.';
use SpockTest qw(
    create_cluster destroy_cluster
    get_test_config scalar_query psql_or_bail
    wait_for_sub_status
);

# Fast, deterministic code-path test: a replica trigger injects SQLSTATE
# 08006 during the first apply attempt.  This is not a process-kill test;
# 105_sub_disable_replay_after_walsender_kill covers the provider crash path.
# The sequence is intentionally nontransactional, so the trigger fails only
# once even though the surrounding INSERT is rolled back.

create_cluster(2, 'Create 2-node cluster for reconnect replay test');

my $config      = get_test_config();
my $node_ports  = $config->{node_ports};
my $host        = $config->{host};
my $dbname      = $config->{db_name};
my $db_user     = $config->{db_user};
my $db_password = $config->{db_password};
my $log_dir     = $config->{log_dir};
my $node_dirs   = $config->{node_datadirs};

my $p1 = $node_ports->[0];
my $p2 = $node_ports->[1];
my $conn_n1 = "host=$host dbname=$dbname port=$p1 user=$db_user password=$db_password";
my $pg_log_n2 = $log_dir =~ m{^/}
    ? "$log_dir/00${p2}.log"
    : "$node_dirs->[1]/$log_dir/00${p2}.log";

psql_or_bail(2, "ALTER SYSTEM SET spock.exception_behaviour = 'sub_disable'");
psql_or_bail(2, "SELECT pg_reload_conf()");

psql_or_bail(1,
    "CREATE TABLE test_disconnect_replay (id INTEGER PRIMARY KEY, val TEXT)");
psql_or_bail(2,
    "CREATE TABLE test_disconnect_replay (id INTEGER PRIMARY KEY, val TEXT)");

psql_or_bail(2, q{
    CREATE SEQUENCE test_disconnect_once_seq;
    CREATE FUNCTION test_disconnect_once() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF nextval('test_disconnect_once_seq') = 1 THEN
            RAISE EXCEPTION USING
                ERRCODE = '08006',
                MESSAGE = 'injected provider connection loss';
        END IF;
        RETURN NEW;
    END
    $$;
    CREATE TRIGGER test_disconnect_once_trigger
        BEFORE INSERT ON test_disconnect_replay
        FOR EACH ROW EXECUTE FUNCTION test_disconnect_once();
    ALTER TABLE test_disconnect_replay
        ENABLE REPLICA TRIGGER test_disconnect_once_trigger;
});

psql_or_bail(2,
    "SELECT spock.sub_create('sub_n1_n2', '$conn_n1', " .
    "ARRAY['default', 'default_insert_only', 'ddl_sql'], false, false)");

ok(wait_for_sub_status(2, 'sub_n1_n2', 'replicating', 30),
    'sub_n1_n2 reaches replicating state');

my $log_offset = -s $pg_log_n2 // 0;

psql_or_bail(1,
    "INSERT INTO test_disconnect_replay VALUES (1, 'must_replay')");

my $row_count = 0;
for (1 .. 60) {
    $row_count = scalar_query(2,
        "SELECT count(*) FROM test_disconnect_replay WHERE id = 1 AND val = 'must_replay'");
    last if defined $row_count && $row_count eq '1';
    select undef, undef, undef, 0.25;
}
is($row_count, '1',
    'transaction is replayed from the last durable origin after connection loss');

is(scalar_query(2,
    "SELECT sub_enabled FROM spock.subscription WHERE sub_name = 'sub_n1_n2'"),
    't', 'subscription remains enabled');

my $new_log = '';
if (open(my $lf, '<', $pg_log_n2)) {
    seek($lf, $log_offset, 0);
    local $/;
    $new_log = <$lf> // '';
    close($lf);
}

unlike($new_log,
    qr/exception handling had no exception\(s\)/,
    'retransmitted transaction does not enter exception replay');

destroy_cluster('Destroy reconnect replay test cluster');

done_testing();
