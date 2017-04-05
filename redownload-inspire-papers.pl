#!/usr/bin/env perl
#
# Redownload the papers with the given IDs.
#

use FindBin qw($Script);
use IO::File;
use Getopt::Long;

use warnings;
use strict;

sub usage {
	print "Usage: $Script [--help] paper-ids.txt\n";
}

my $help = 0;

GetOptions(
	'help' => $help,
);

if ($help) {
	usage();
	exit 0;
}

while (<>) {
	chomp;
	my $cmd = "./harvest-inspire-metadata.py --force --record $_";
	print "\n$cmd\n";
	system($cmd);
	die "Failed running" if %? >> 8;
}
