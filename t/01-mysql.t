#!/usr/bin/env perl

use warnings;
use strict;
use Test::More tests => 8;
use Pye::SQL;

my $pye;

eval {
	$pye = Pye::SQL->new(
		db_type => 'mysql',
		database => 'test',
		table => 'pye_test',
		username => 'root',
		password => '',
		be_safe => 1
	);
};

SKIP: {
	if ($@) {
		diag("Skipped: $@.");
		skip("MySQL needs to be running for this test.", 8);
	}

	ok($pye, "Pye::SQL object created");

	$pye->{dbh}->do('CREATE TABLE pye_test (
		session_id VARCHAR(60) NOT NULL,
		date DATETIME(6) NOT NULL,
		text VARCHAR(128) NOT NULL,
		data TEXT
	)');

	$pye->{dbh}->do('CREATE INDEX logs_for_session ON pye_test (session_id)');

	ok($pye->log(1, "What's up?"), "Simple log message");

	ok($pye->log(1, "Some data", { hey => 'there' }), "Log message with data structure");

	sleep(0.5);

	ok($pye->log(2, "Yo yo ma"), "Log message for another session");

	my @latest_sessions = $pye->list_sessions;

	is(scalar(@latest_sessions), 2, "We only have one session");

	is($latest_sessions[0]->{id}, '2', "We have the correct session ID");

	my @logs = $pye->session_log(1);

	is(scalar(@logs), 2, 'Session has two log messages');

	ok(exists $logs[1]->{data} && $logs[1]->{data}->{hey} eq 'there', 'Second log message has a data element');

	#$pye->_remove_session_logs(1);
}

done_testing();
