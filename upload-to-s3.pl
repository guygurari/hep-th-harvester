#!/usr/bin/perl
# 
# Upload processed hep-th papers to S3 and cleanup.
#

use warnings;
use strict;
use IO::File;

my $s3 = "$ENV{HOME}/s3cmd-master/s3cmd";
my $done_filename = "done-arxiv-pdfs.txt";
my $bucket_url = "s3://hep-th";
my $sleep_secs = 60;

while (1) {
    my $f = IO::File->new("< $done_filename") || die;
    my $prev_id;

# Process all chunk IDs except for the last one, because the last
# one may still be in progress. Each ID corresponds to a different directory.
# So if we see a new ID in the 'done' file, that means all previous IDs
# have finished, and we can upload their directories to S3.
    while (my $line = <$f>) {
        chomp $line;
        $line =~ m|s3://arxiv/pdf/arXiv_pdf_(\d+)_(\d+).tar| || die;
        my $id = $1;

        if (defined $prev_id && $id != $prev_id) {
            my $dir = $prev_id;

            if (-d $dir) {
                print "\n>>> $dir : uploading to S3\n";
                upload_chunk_to_s3($dir);
            }
            else {
                print "\n>>> $dir : already uploaded, skipping";
            }
        }

        $prev_id = $id;
    }

    $f->close();
    print "\n<<< Sleeping for $sleep_secs seconds ...\n";
    sleep $sleep_secs;
    print "\n>>> I'm up!\n";
}

sub upload_chunk_to_s3 {
    my $id = shift;
    my $compressed = "$id.tbz";

    execute("tar cvjf $compressed $id/*.pdf");
    execute("s3cmd put --acl-public $compressed $bucket_url");
    execute("rm $compressed");
    execute("rm -r $id");

}

sub execute {
    my $cmd = shift;
    print "$cmd\n";
    system($cmd);
    my $rc = $? >> 8;
    die "Error running command: $@" if $rc;
}

