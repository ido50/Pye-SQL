package Pye::SQL;

# ABSTRACT: Session-based logging platform on top of MongoDB

use warnings;
use strict;

use Carp;
use DBI;
use JSON::MaybeXS qw/JSON/;
use Role::Tiny::With;

our $VERSION = "1.000000";
$VERSION = eval $VERSION;

with 'Pye';

sub new {
	my ($class, %opts) = @_;

	croak "You must provide the database type (db_type), one of 'mysql' or 'pgsql'"
		unless $opts{db_type} &&
			$opts{db_type} =~ m/^(my|pg)sql$/i;

	$opts{db_type} = lc($opts{db_type});

	return bless {
		dbh => DBI->connect(
			_build_dsn(\%opts),
			$opts{username},
			$opts{password},
			{
				AutoCommit => 1,
				RaiseError => $opts{be_safe} ? 1 : 0
			}
		),
		json => JSON->new->allow_blessed->convert_blessed,
		db_type => $opts{db_type},
		table => $opts{table} || 'logs'
	}, $class;
}

=head1 OBJECT METHODS

=head2 log( $session_id, $text, [ \%data ] )

Inserts a new log message to the database, for the session with the supplied
ID and with the supplied text. Optionally, a hash-ref of supporting data can
be attached to the message.

You should note that for consistency, the session ID will always be stored in
the database as a string, even if it's a number.

If a data hash-ref has been supplied, C<Pye> will make sure (recursively)
that no keys of that hash-ref have dots in them, since MongoDB will refuse to
store such hashes. All dots found will be replaced with semicolons (";").

=cut

sub log {
	my ($self, $sid, $text, $data) = @_;

	my $now = $self->{db_type} eq 'mysql' ? 'NOW(6)' : 'NOW()';

	$self->{dbh}->do(
		"INSERT INTO $self->{table} VALUES (?, $now, ?, ?)",
		undef, "$sid", $text, $data ? $self->{json}->encode($data) : undef
	);
}

=head2 session_log( $session_id )

Returns all log messages for the supplied session ID, sorted by date in ascending
order.

=cut

sub session_log {
	my ($self, $session_id) = @_;

	my $sth = $self->{dbh}->prepare("SELECT date, text, data FROM $self->{table} WHERE session_id = ? ORDER BY date ASC");
	$sth->execute("$session_id");

	my @msgs;
	while (my $row = $sth->fetchrow_hashref) {
		$row->{data} = $self->{json}->decode($row->{data})
			if $row->{data};
		push(@msgs, $row);
	}

	$sth->finish;

	return @msgs;
}

=head2 list_sessions( [ \%opts ] )

Returns a list of sessions, sorted by the date of the first message logged for each
session in descending order. If no options are provided, the latest 10 sessions are
returned. The following options are allowed:

=over

=item * B<skip>

How many sessions to skip, defaults to 0.

=item * B<limit>

How many sessions to list, defaults to 10.

=item * B<sort>

The sorting of the sessions (as an ORDER BY clause). Defaults to 'date DESC'.

=back

=cut

sub list_sessions {
	my ($self, $opts) = @_;

	$opts			||= {};
	$opts->{skip}	||= 0;
	$opts->{limit}	||= 10;
	$opts->{sort}	||= 'date DESC';

	my $sth = $self->{dbh}->prepare("SELECT session_id AS id, MIN(date) AS date FROM $self->{table} GROUP BY id ORDER BY $opts->{sort} LIMIT $opts->{limit} OFFSET $opts->{skip}");
	$sth->execute;

	my @sessions;
	while (my $row = $sth->fetchrow_hashref) {
		push(@sessions, $row);
	}

	$sth->finish;

	return @sessions;
}

#####################################
# _remove_session_logs($session_id) #
#===================================#
# removes all log messages for the  #
# supplied session ID.              #
#####################################

sub _remove_session_logs {
	my ($self, $session_id) = @_;

	$self->{dbh}->do("DELETE FROM $self->{table} WHERE session_id = ?", undef, "$session_id");
}

sub _build_dsn {
	my $opts = shift;

	if ($opts->{db_type} eq 'mysql') {
		'DBI:mysql:database='.
			($opts->{database} || 'logs').
				';host='.($opts->{hostname} || '127.0.0.1').
					';port='.($opts->{port} || 3306).
						';mysql_enable_utf8=1';
	} else {
		# pgsql
		'dbi:Pg:dbname='.
			($opts->{database} || 'logs').
				';host='.($opts->{hostname} || '127.0.0.1').
					';port='.($opts->{port} || 5432);
	}
}

=head1 CONFIGURATION AND ENVIRONMENT
  
C<Pye> requires no configuration files or environment variables.

=head1 DEPENDENCIES

C<Pye> depends on the following CPAN modules:

=over

=item * Carp

=item * DBI

=item * JSON::MaybeXS

=item * Role::Tiny

=back

Using C<Pye> with MySQL will also require C<DBD::mysql>. PostgreSQL will
require C<DBD::Pg>.

=head1 BUGS AND LIMITATIONS

Please report any bugs or feature requests to
C<bug-Pye-SQL@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Pye-SQL>.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

	perldoc Pye::SQL

You can also look for information at:

=over 4
 
=item * RT: CPAN's request tracker
 
L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Pye-SQL>
 
=item * AnnoCPAN: Annotated CPAN documentation
 
L<http://annocpan.org/dist/Pye-SQL>
 
=item * CPAN Ratings
 
L<http://cpanratings.perl.org/d/Pye-SQL>
 
=item * Search CPAN
 
L<http://search.cpan.org/dist/Pye-SQL/>
 
=back
 
=head1 AUTHOR
 
Ido Perlmuter <ido@ido50.net>
 
=head1 LICENSE AND COPYRIGHT
 
Copyright (c) 2015, Ido Perlmuter C<< ido@ido50.net >>.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself, either version
5.8.1 or any later version. See L<perlartistic|perlartistic>
and L<perlgpl|perlgpl>.
 
The full text of the license can be found in the
LICENSE file included with this module.
 
=head1 DISCLAIMER OF WARRANTY
 
BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.
 
IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.

=cut

1;
__END__
