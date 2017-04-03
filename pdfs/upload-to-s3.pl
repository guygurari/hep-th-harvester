#!/usr/bin/perl
# 
# Upload processed hep-th papers to S3 and cleanup.
#

use warnings;
use strict;
use IO::File;
use Getopt::Long;
use FindBin qw($Script);

my $s3 = "$ENV{HOME}/s3cmd-master/s3cmd";
my $done_filename = "done-arxiv-pdfs.txt";
my $bucket_url = "s3://hep-th";
my $sleep_secs = 60;

sub usage {
    print "Usage: $Script [--all] [--once] [--help]\n\n";
    print "--all - upload all directories. if this is not specified, only\n";
    print "        uploads directories that have clearly finished\n";
    print "        downloading.\n";
    print "\n";
    print "--once - just run once, don't keep monitoring for new directories.\n";
    print "\n";
}

my $help = 0;
my $upload_all = 0;
my $run_once = 0;

GetOptions('help' => \$help, 'all' => \$upload_all, 'once' => \$run_once);

if ($help) {
    usage();
    exit 0;
}

while (1) {
    my $done_f = IO::File->new("< $done_filename") || die;
    my $prev_id;

# Process all chunk IDs except for the last one, because the last
# one may still be in progress. Each ID corresponds to a different directory.
# So if we see a new ID in the 'done' file, that means all previous IDs
# have finished, and we can upload their directories to S3.
    while (my $line = <$done_f>) {
        chomp $line;
        $line =~ m|s3://arxiv/pdf/arXiv_pdf_(\d+)_(\d+).tar| || die;
        my $id = $1;

        if ($upload_all) {
            process_dir($id);
        }
        else {
            if (defined $prev_id && $id != $prev_id) {
                process_dir($prev_id);
            }

            $prev_id = $id;
        }
    }

    if ($run_once) {
        print "\n";
        last;
    }

    $done_f->close();
    print "\n<<< Sleeping for $sleep_secs seconds ...\n";
    sleep $sleep_secs;
    print "\n>>> I'm up!\n";
}

sub process_dir {
    my $dir = shift;

    if (-d $dir) {
        print "\n>>> $dir : uploading to S3\n";
        upload_chunk_to_s3($dir);
    }
    else {
        print "\n>>> $dir : already uploaded, skipping";
    }
}

sub upload_chunk_to_s3 {
    my $id = shift;
    my $tarball = "$id.tbz";

    my $files_pat = "$id/*.pdf";
    my @files_to_upload = glob($files_pat);

    if (scalar(@files_to_upload) > 0)  {
        execute("tar cvjf $tarball $files_pat");
        execute("s3cmd put --acl-public $tarball $bucket_url");
        execute("rm $tarball");
    }

    execute("rm -r $id");
}

sub execute {
    my $cmd = shift;
    print "$cmd\n";
    system($cmd);
    my $rc = $? >> 8;
    die "Error running command: $@" if $rc;
}

