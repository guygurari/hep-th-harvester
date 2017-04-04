#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw($Script $Dir);
use IO::File;
use Arxiv;

my $category = 'hep-th';
my $s3 = "$ENV{HOME}/s3cmd-master/s3cmd";
my $done_filename = "done-arxiv-pdfs.txt";

my $chunk_list_file = IO::File->new("< arxiv-pdfs.txt") || die;
my $done = read_done_chunks();
my $done_file = IO::File->new(">> $done_filename");

while (my $line = <$chunk_list_file>) {
    chomp $line;
    $line =~ /s3:.*tar/ || next;
    my $chunk_url = $&;
    print "$chunk_url\n";
    my $chunk_file = $chunk_url;
    $chunk_file =~ s/.*\///;

    if (exists $done->{$chunk_url}) {
        print ">>> Chunk $chunk_url done, skipping\n\n";
        next;
    }
    elsif (-f $chunk_file) {
        print ">>> Chunk file already exists\n\n";
    }
    else {
        print ">>> Downloading $chunk_url ...\n\n";
        execute("$s3 get --requester-pays $chunk_url");
    }

    die "Missing file $chunk_file" unless -f $chunk_file;

    print ">>> Unpacking $chunk_file ...\n\n";
    my $tar = IO::File->new("tar xvf $chunk_file |") || die;
    my $maybe_pdf_filename;
    my @pdfs;

    while ($maybe_pdf_filename = <$tar>) {
        chomp $maybe_pdf_filename;

        if ($maybe_pdf_filename =~ /\.pdf/) {
            push @pdfs, $maybe_pdf_filename;
        }
    }

    print ">>> Filtering $chunk_file ...\n\n";
    foreach my $pdf (@pdfs) {
        print "$pdf .. ";

        if ( Arxiv::is_pdf_in_category($pdf, $category)) {
            print " in $category\n";
        }
        else {
            print "not in category, deleting\n";
            unlink $pdf || die;
        }
    }

    unlink $chunk_file;
    $done_file->print("$chunk_url\n");
    print "\n\n";
}

$chunk_list_file->close();
$done_file->close();

sub execute {
    my $cmd = shift;
    print "$cmd\n\n";
    system($cmd);
    my $rc = $? >> 8;
    die "Error running command: $@" if $rc;
}

sub read_done_chunks {
    local $_;
    my $done_file = IO::File->new("< $done_filename");
    my $done = {};
    while (<$done_file>) {
        chomp;
        $done->{$_} = $1;
    }
    return $done;
}
