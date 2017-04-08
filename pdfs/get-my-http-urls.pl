#!/usr/bin/env perl
# 
# Get the list of hep-th URLs on our S3 bucket.
#

use strict;
use warnings;
use IO::File;

my $in = IO::File->new("s3cmd ls 's3://hep-th/pdfs/*.pdf' |");
my $out = IO::File->new("> hep-th-urls.txt");

while (<$in>) {
    chomp;
    s|.*s3://|https://s3.amazonaws.com/|;
    $out->print("$_\n");
}

$in->close();
$out->close();

