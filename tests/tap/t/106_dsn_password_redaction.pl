use strict;
use warnings;
use IPC::Run qw(run);
use Test::More;
use lib '.';
use SpockTest qw(
    create_cluster destroy_cluster
    system_maybe system_or_bail
    get_test_config scalar_query psql_or_bail
    wait_for_pg_ready wait_for_sub_status
);

# A provider outage makes the apply worker reconnect using the provider DSN
# stored in the subscription catalog.  Historically both the DEBUG1 reconnect
# message and the connection failure DETAIL printed that DSN verbatim,
# including its password.  Exercise the real background-worker reconnect path
# and prove that useful failure/recovery diagnostics remain without exposing a
# synthetic credential.

create_cluster(2, 'Create 2-node cluster for DSN redaction test');

my $config      = get_test_config();
my $ports       = $config->{node_ports};
my $node_dirs   = $config->{node_datadirs};
my $host        = $config->{host};
my $dbname      = $config->{db_name};
my $db_user     = $config->{db_user};
my $pg_bin      = $config->{pg_bin};
my $log_dir     = $config->{log_dir};
my $test_log    = $config->{log_file};

my $p1 = $ports->[0];
my $p2 = $ports->[1];
my $secret = 'test-password';
my $provider_dsn =
    "host=$host dbname=$dbname port=$p1 user=$db_user " .
    "password=$secret sslmode=disable";
my $subscriber_log = $log_dir =~ m{^/}
    ? "$log_dir/00${p2}.log"
    : "$node_dirs->[1]/$log_dir/00${p2}.log";

# spockctrl is installed by the same top-level build as the extension.  Prove
# that its connection-failure path keeps a useful error without echoing the
# complete conninfo read from its configuration file.
my $spockctrl = "$pg_bin/spockctrl";
my $spockctrl_config = "$node_dirs->[0]/spockctrl-redaction.json";
open(my $config_fh, '>', $spockctrl_config)
    or die "cannot write $spockctrl_config: $!";
print {$config_fh} <<"JSON";
{
  "global": {
    "spock": {"cluster_name": "redaction", "version": "1.0.0"},
    "log": {"log_level": "ERROR", "log_destination": "console",
            "log_file": "$node_dirs->[0]/spockctrl-redaction.log"}
  },
  "spock-nodes": [{
    "node_name": "redaction-target",
    "postgres": {"postgres_ip": "$host", "postgres_port": 1,
                 "postgres_user": "$db_user", "postgres_password": "$secret",
                 "postgres_db": "$dbname"}
  }]
}
JSON
close($config_fh);

ok(-x $spockctrl, 'spockctrl is installed by the tested Spock build');
my ($spockctrl_stdout, $spockctrl_stderr) = ('', '');
run([$spockctrl, 'node', 'pg-version', '--node=redaction-target',
     "--config=$spockctrl_config"],
    '>', \$spockctrl_stdout, '2>', \$spockctrl_stderr);
my $spockctrl_output = $spockctrl_stdout . $spockctrl_stderr;
like($spockctrl_output, qr/Connection to database failed/,
     'spockctrl retains the useful connection-failure diagnostic');
like($spockctrl_output, qr/connection details redacted/,
     'spockctrl explicitly reports that connection details were redacted');
ok(index($spockctrl_output, $secret) == -1,
   'spockctrl connection failure does not expose its configured password');

psql_or_bail(1,
    'CREATE TABLE test_dsn_redaction (id integer PRIMARY KEY, payload text)');
psql_or_bail(2,
    'CREATE TABLE test_dsn_redaction (id integer PRIMARY KEY, payload text)');

psql_or_bail(2,
    "SELECT spock.sub_create('sub_dsn_redaction', '$provider_dsn', " .
    "ARRAY['default', 'default_insert_only', 'ddl_sql'], false, false)");
ok(wait_for_sub_status(2, 'sub_dsn_redaction', 'replicating', 30),
   'subscription starts with the synthetic password in its provider DSN');

# Ignore setup logging.  The evidence below is generated only by the apply
# worker reconnecting from the catalog after the provider has stopped; no SQL
# statement containing the sentinel is executed after this offset.
my $log_offset = -s $subscriber_log // 0;

system_or_bail("$pg_bin/pg_ctl", '-D', $node_dirs->[0],
               '-m', 'fast', '-w', 'stop');

my $new_log = '';
for (1 .. 60) {
    if (open(my $lf, '<', $subscriber_log)) {
        seek($lf, $log_offset, 0);
        local $/;
        $new_log = <$lf> // '';
        close($lf);
    }
    last if $new_log =~ /could not connect to the postgresql server/;
    sleep(1);
}

like($new_log, qr/could not connect to the postgresql server/,
     'provider outage retains a useful connection-failure diagnostic');
like($new_log, qr/connection details were redacted/,
     'provider failure explicitly reports that connection details were redacted');
ok(index($new_log, $secret) == -1,
   'provider reconnect logs do not expose the DSN password');
ok(index($new_log, 'dsn was:') == -1,
   'provider reconnect logs do not print the raw DSN');

system_or_bail("$pg_bin/pg_ctl", '-D', $node_dirs->[0],
               '-l', $test_log, '-w', 'start');
ok(wait_for_pg_ready($host, $p1, $pg_bin, 30),
   'provider accepts connections after restart');
ok(wait_for_sub_status(2, 'sub_dsn_redaction', 'replicating', 60),
   'subscription reconnects after provider restart');

psql_or_bail(1,
    "INSERT INTO test_dsn_redaction VALUES (1, 'recovered')");
my $payload = '';
for (1 .. 60) {
    $payload = scalar_query(2,
        'SELECT payload FROM test_dsn_redaction WHERE id = 1');
    last if defined $payload && $payload eq 'recovered';
    sleep(1);
}
is($payload, 'recovered',
   'replication recovers after the redacted connection failure');

system_maybe("$pg_bin/psql", '-h', $host, '-p', $p2,
    '-U', $db_user, '-d', $dbname,
    '-c', "SELECT spock.sub_drop('sub_dsn_redaction')");

destroy_cluster('Destroy DSN redaction test cluster');
done_testing();
