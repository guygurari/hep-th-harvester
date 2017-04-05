#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw($Script $Dir);
use IO::File;
use Getopt::Long;

use Arxiv;

sub usage {
	print "Usage: $Script [--help] [--upload-to-s3]\n";
}

my $help = 0;
my $upload_to_s3 = 0;

GetOptions(
	'help' => \$help,
	'upload-to-s3' => \$upload_to_s3,
);

if ($help) {
	usage();
	exit 0;
}

my $category = 'hep-th';
my $s3 = "$ENV{HOME}/s3cmd-master/s3cmd";
my $upload_bucket_url = "s3://hep-th/pdfs/";
my $num_upload_retries = 3;
my $done_filename = "done-arxiv-pdfs.txt";

print "Updating list of arxiv PDF chunks...\n";
execute("./download-arxiv-pdf-list");

my $chunk_list_file = IO::File->new("< arxiv-pdfs.txt") || die;
my $done = read_done_chunks();
my $done_file = IO::File->new(">> $done_filename");

while (my $line = <$chunk_list_file>) {
    chomp $line;
    $line =~ /s3:.*tar/ || next;
    my $chunk_url = $&;
    #print "$chunk_url\n";
    my $chunk_file = $chunk_url;
    $chunk_file =~ s/.*\///;

    if (exists $done->{$chunk_url}) {
        print ">>> Chunk $chunk_url done, skipping\n\n";
        next;
    }
    #elsif (-f $chunk_file) {
        #print ">>> Chunk file already exists, deleting\n\n";
		#unlink $chunk_file;
    #}

	print ">>> Downloading $chunk_url ...\n\n";
	execute("$s3 get --requester-pays --continue $chunk_url");

    die "Missing file $chunk_file" unless -f $chunk_file;

    print ">>> Unpacking $chunk_file ...\n\n";
    my $tar = IO::File->new("tar xvf $chunk_file |") || die;
    my $maybe_pdf_filename;
	my $tar_subdir;
    my @pdfs;

    while ($maybe_pdf_filename = <$tar>) {
        chomp $maybe_pdf_filename;

        if ($maybe_pdf_filename =~ /\.pdf/) {
            push @pdfs, $maybe_pdf_filename;
        }

		my $subdir = $maybe_pdf_filename;
		$subdir =~ s/\/.*$//;

		if (-d $subdir) {
			$tar_subdir = $subdir;
		}
    }

    print ">>> Filtering $chunk_file ...\n\n";
    foreach my $pdf (@pdfs) {
        print "$pdf .. ";

        if (Arxiv::is_pdf_in_category($pdf, $category)) {
			if ($upload_to_s3) {
				print "in $category, uploading\n";
				execute("$s3 --acl-public --quiet --no-mime-magic put $pdf $upload_bucket_url",
					    $num_upload_retries);
				unlink $pdf || die;
			}
			else {
				print " in $category, keeping\n";
			}
		}
		else {
            print "not in category, deleting\n";
            unlink $pdf || die;
        }
    }

	# Delete the subdirs created by tar
	if ($upload_to_s3 && defined $tar_subdir) {
		rmdir $tar_subdir || warn "Failed to delete directory '$tar_subdir'";
	}

    unlink $chunk_file;
    $done_file->print("$chunk_url\n");
    print "\n\n";
}

$chunk_list_file->close();
$done_file->close();

sub execute {
    my ($cmd, $num_attempts) = @_;
    $num_attempts = 1 unless defined $num_attempts;
    my $rc;

    while ($num_attempts > 0) {
        print "Running: $cmd\n\n";
        system($cmd);
        $rc = $? >> 8;
        last if $rc == 0;

        $num_attempts--;

        if ($num_attempts == 0) {
            die "Error running command: $@" if $rc;
        }
        else {
            print "Command failed, retrying... Error: $@\n";
        }
    }
}

sub read_done_chunks {
    local $_;

    if (! -f $done_filename) {
        return {};
    }

    my $done_file = IO::File->new("< $done_filename");
    my $done = {};
    while (<$done_file>) {
        chomp;
        $done->{$_} = $1;
    }
    return $done;
}
