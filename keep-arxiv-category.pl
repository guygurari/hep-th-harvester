#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw($Script);
use Getopt::Long;
use IO::File;
use ArxivPdf;

sub usage {
    print "Usage: $Script [--help] [--category hep-th] [--delete] file1.pdf file2.pdf ...\n\n";
	print "--delete : delete PDFs that are not in the category\n\n";
}

my $help = 0;
my $category = 'hep-th';
my $delete = 0;

GetOptions(
    'help' => \$help,
	'category=s' => \$category,
	'delete' => \$delete,
);

if ($help) {
    usage();
    exit 0;
}

print join(' ', @ARGV) . "\n";

if (scalar(@ARGV) < 1 || !defined $category) {
    usage();
    exit 1;
}

my @files = @ARGV;

foreach my $pdf (@files) {
	print "$pdf ... ";
	if (ArxivPdf::is_pdf_in_category($pdf, $category)) {
		print "in $category !!!\n";
	}	
	else {
		if ($delete) {
			print "not in category, deleting\n";
			unlink $pdf;
		}
        else {
			print "not in category\n";
        }
	}
}


