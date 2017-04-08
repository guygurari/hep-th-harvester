#!/usr/bin/env perl
# 
# Get the list of hep-th URLs on our S3 bucket.
#

use strict;
use warnings;
use IO::File;

my $in = IO::File->new("s3cmd ls 's3://hep-th/pdfs/' |");
my $s3_out = IO::File->new("> hep-th-pdf-s3-list.txt");
my $url_out = IO::File->new("> hep-th-pdf-urls.txt");
my $id_out = IO::File->new("> hep-th-pdf-ids.txt");

while (<$in>) {
    chomp;
    next unless /\.pdf$/;
    $s3_out->print("$_\n");

    my $url = $_;
    $url =~ s|.*s3://|https://s3.amazonaws.com/|;
    $url_out->print("$url\n");

    m|.*pdfs\/(.*)\.pdf|;
    my $id = $1;
    $id_out->print("$id\n");
}

$in->close();
$s3_out->close();
$url_out->close();
$id_out->close();

