#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use FindBin qw($Script);
use Data::Dumper;

my $dbfile = 'hep-th.sqlite';

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","") || die;

sub usage {
    print "Usage: $Script arxiv-id\n\n";
}

if (scalar(@ARGV) != 1) {
    usage();
    exit 1;
}

my $id = $ARGV[0];

my $sth = $dbh->prepare("SELECT * FROM arxiv_papers where id='$id'");
$sth->execute();
my $row = $sth->fetch();

if (defined $row) {
    print Dumper($row);
}
else {
    print "Paper $id not found\n";
}

