#!/usr/bin/perl
# =============================================================================
# Test: 015_forward_origin_advance.pl - Verify Forward Origin Tracking
# =============================================================================
# This test verifies that when forward_origins='all' is set and a disabled
# subscription is pre-created on n3 to n1 (simulating the bidirectional JOIN
# procedure Step 16b), the apply worker on n3 correctly advances that
# pre-created origin as forwarded n1 transactions arrive.
#
# Topology:
#   n1 -> n2 -> n3
#            forward_origins='all' on both subscriptions
#
# Expected behavior:
# - Before catchup: disabled sub_n3_n1 created on n3 via sub_create(enabled=false)
#   This creates a named replication origin at LSN 0/0.
# - n1 inserts data; n2 receives and forwards to n3
# - n3 advances the pre-created origin LSN as forwarded n1 transactions arrive
# - When sub_enable('sub_n3_n1') later fires, the apply worker starts from
#   the correct position — no duplicates, no gaps
# =============================================================================

use strict;
use warnings;
use Test::More tests => 26;
use lib '.';
use SpockTest qw(create_cluster destroy_cluster system_or_bail get_test_config scalar_query psql_or_bail);

# =============================================================================
# SETUP: Create 3-node cluster
# =============================================================================

create_cluster(3, 'Create 3-node cluster');

my $config     = get_test_config();
my $node_ports = $config->{node_ports};
my $dbname     = $config->{db_name};
my $host       = $config->{host};

my $conn_n1 = "host=$host port=$node_ports->[0] dbname=$dbname";
my $conn_n2 = "host=$host port=$node_ports->[1] dbname=$dbname";

# =============================================================================
# TEST: Forward Origin Advance
# =============================================================================

# Create replication sets on all nodes
psql_or_bail(1, "SELECT spock.repset_create('cascade_set')");
psql_or_bail(2, "SELECT spock.repset_create('cascade_set')");
psql_or_bail(3, "SELECT spock.repset_create('cascade_set')");
pass('Created replication sets');

# Create test table on all nodes
psql_or_bail(1, "CREATE TABLE test_origin (id serial primary key, val text)");
psql_or_bail(2, "CREATE TABLE test_origin (id serial primary key, val text)");
psql_or_bail(3, "CREATE TABLE test_origin (id serial primary key, val text)");
pass('Created test table on all nodes');

# Add table to replication sets
psql_or_bail(1, "SELECT spock.repset_add_table('cascade_set', 'test_origin')");
psql_or_bail(2, "SELECT spock.repset_add_table('cascade_set', 'test_origin')");
psql_or_bail(3, "SELECT spock.repset_add_table('cascade_set', 'test_origin')");
pass('Added table to replication sets');

# Create cascade: n2 subscribes to n1 with forward_origins='all'
psql_or_bail(2, "SELECT spock.sub_create('sub_n2_n1', '$conn_n1', ARRAY['cascade_set'], false, false, ARRAY['all'])");
pass('Created subscription n2->n1 with forward_origins=all');

# Create cascade: n3 subscribes to n2 with forward_origins='all'
psql_or_bail(3, "SELECT spock.sub_create('sub_n3_n2', '$conn_n2', ARRAY['cascade_set'], false, false, ARRAY['all'])");
pass('Created subscription n3->n2 with forward_origins=all');

# Pre-create disabled subscription on n3 to n1 — Step 16b of the bidirectional
# JOIN procedure.  sub_create(enabled=false) creates a named replication origin
# on n3 at LSN 0/0 without starting an apply worker.  maybe_advance_forwarded_origin()
# advances it as forwarded n1 transactions arrive during catchup, so that
# sub_enable('sub_n3_n1') starts from the correct LSN.
psql_or_bail(3, "SELECT spock.sub_create(
    subscription_name := 'sub_n3_n1',
    provider_dsn      := '$conn_n1',
    replication_sets  := ARRAY['cascade_set'],
    synchronize_structure := false,
    synchronize_data  := false,
    enabled           := false
)");
pass('Pre-created disabled subscription on n3 to n1 (Step 16b)');

# Wait for subscriptions to be ready
system_or_bail 'sleep', '5';

# Verify subscriptions are replicating
my $sub_n2 = scalar_query(2, "SELECT 1 FROM spock.sub_show_status() WHERE subscription_name = 'sub_n2_n1' AND status = 'replicating'");
is($sub_n2, '1', 'Subscription n2->n1 is replicating');

my $sub_n3 = scalar_query(3, "SELECT 1 FROM spock.sub_show_status() WHERE subscription_name = 'sub_n3_n2' AND status = 'replicating'");
is($sub_n3, '1', 'Subscription n3->n2 is replicating');

my $sub_disabled = scalar_query(3, "SELECT 1 FROM spock.sub_show_status() WHERE subscription_name = 'sub_n3_n1' AND status = 'disabled'");
is($sub_disabled, '1', 'Subscription sub_n3_n1 is disabled on n3');

# Insert data on n1 - this will be forwarded through n2 to n3
psql_or_bail(1, "INSERT INTO test_origin (val) VALUES ('from_node_n1')");
system_or_bail 'sleep', '5';

# Verify data reached n3
my $count_n3 = scalar_query(3, "SELECT COUNT(*) FROM test_origin WHERE val = 'from_node_n1'");
is($count_n3, '1', 'Data from n1 reached n3 via n2');

# =============================================================================
# KEY TEST: Check that n3's pre-created origin for sub_n3_n1 has been advanced
# =============================================================================
# sub_create(enabled=false) created the origin at 0/0.
# As forwarded n1 transactions arrive on n3, maybe_advance_forwarded_origin()
# looks up sub_n3_n1 by n1's node OID and advances its named origin.
# When sub_enable('sub_n3_n1') fires, the apply worker reads this origin
# and starts replication from the already-advanced position.

my $expected_origin = scalar_query(3,
    "SELECT spock.spock_gen_slot_name(current_database()::name, 'n1'::name, 'sub_n3_n1'::name)");
diag("Expected forwarded origin name on n3: $expected_origin");

my $origin_exists = scalar_query(3,
    "SELECT COUNT(*) FROM pg_replication_origin WHERE roname = '$expected_origin'");
diag("Origin '$expected_origin' exists on n3: $origin_exists");
is($origin_exists, '1', "n3 has replication origin for forwarded source ($expected_origin)");

# Verify the origin has been advanced beyond 0/0
my $origin_lsn = scalar_query(3,
    "SELECT COALESCE(s.remote_lsn::text, 'NULL')
     FROM pg_replication_origin o
     LEFT JOIN pg_replication_origin_status s ON o.roident = s.local_id
     WHERE o.roname = '$expected_origin'");
diag("Origin '$expected_origin' LSN on n3: $origin_lsn");
ok($origin_lsn ne '0/0' && $origin_lsn ne 'NULL' && $origin_lsn ne '',
    "Origin $expected_origin has been advanced (LSN is valid)");

# =============================================================================
# GAP DETECTION TEST: Demonstrate origin tracking enables gap detection
# =============================================================================
# With forwarded origin tracking:
#   - We can query n3's position relative to n1 via the forwarded origin LSN
#   - We can query n1's current position: pg_current_wal_lsn()
#   - If n1's LSN > n3's tracked LSN, there's unreplicated data (a "gap")
#   - This enables tooling to make informed switchover decisions

diag("=== GAP DETECTION TEST: Origin tracking enables gap detection ===");

# Insert more data and let it propagate
psql_or_bail(1, "INSERT INTO test_origin (val) VALUES ('batch2_row1')");
psql_or_bail(1, "INSERT INTO test_origin (val) VALUES ('batch2_row2')");
system_or_bail 'sleep', '3';

# Verify data reached n3
my $count_after_batch2 = scalar_query(3, "SELECT COUNT(*) FROM test_origin");
diag("Row count on n3 after batch 2: $count_after_batch2");
is($count_after_batch2, '3', 'n3 has 3 rows after batch 2');

# Capture n3's current tracked LSN for n1
my $c_origin_lsn = scalar_query(3,
    "SELECT COALESCE(s.remote_lsn::text, 'not_tracked')
     FROM pg_replication_origin o
     LEFT JOIN pg_replication_origin_status s ON o.roident = s.local_id
     WHERE o.roname = '$expected_origin'");
diag("n3's tracked LSN for n1 (origin '$expected_origin'): $c_origin_lsn");

ok($c_origin_lsn ne 'not_tracked' && $c_origin_lsn ne '',
   'n3 can track its position relative to n1 (gap detection enabled)');

# Simulate gap: disable n2->n1, insert on n1, check that origin does not advance
diag("Creating gap: disabling n2->n1 subscription...");
psql_or_bail(2, "SELECT spock.sub_disable('sub_n2_n1')");
system_or_bail 'sleep', '2';

# Insert data on n1 that creates a gap (cannot reach n3)
psql_or_bail(1, "INSERT INTO test_origin (val) VALUES ('gap_data')");
system_or_bail 'sleep', '5';

# n3's origin LSN should still be at the old position (gap exists)
my $c_origin_after_gap = scalar_query(3,
    "SELECT COALESCE(s.remote_lsn::text, 'not_tracked')
     FROM pg_replication_origin o
     LEFT JOIN pg_replication_origin_status s ON o.roident = s.local_id
     WHERE o.roname = '$expected_origin'");
diag("n3's origin LSN (unchanged, gap detected): $c_origin_after_gap");

# Verify the LSNs show a gap (n1 advanced, n3's tracking hasn't)
is($c_origin_after_gap, $c_origin_lsn, 'Gap detected: n3 origin unchanged while n1 advanced');

# Clean test: verify n3 still has 3 rows (gap data didn't arrive)
my $count_with_gap = scalar_query(3, "SELECT COUNT(*) FROM test_origin");
is($count_with_gap, '3', 'n3 still has 3 rows (gap data not received)');

# =============================================================================
# GAP RECOVERY TEST: Re-enable n2->n1 and verify gap data arrives on n3
# =============================================================================
# When the broken link is restored, the gap row ('gap_data') must flow through
# n2->n3 and n3's forwarded origin LSN must advance past the previous ceiling.

psql_or_bail(2, "SELECT spock.sub_enable('sub_n2_n1')");
system_or_bail 'sleep', '5';

my $count_after_reenable = scalar_query(3, "SELECT COUNT(*) FROM test_origin");
is($count_after_reenable, '4', 'n3 received gap_data after n2->n1 re-enabled');

my $c_origin_after_reenable = scalar_query(3,
    "SELECT COALESCE(s.remote_lsn::text, 'not_tracked')
     FROM pg_replication_origin o
     LEFT JOIN pg_replication_origin_status s ON o.roident = s.local_id
     WHERE o.roname = '$expected_origin'");
diag("n3's origin LSN after re-enable: $c_origin_after_reenable");
ok($c_origin_after_reenable ne $c_origin_lsn,
   'n3 forwarded origin LSN advanced after gap closed');

# =============================================================================
# CLEANUP
# =============================================================================

destroy_cluster('Cleanup');
