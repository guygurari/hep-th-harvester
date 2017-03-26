package ArxivPdf;

use IO::File;

my $ghostscript = "$ENV{HOME}/gs/gs-921-linux-x86_64";
my $paper_ids;

sub load_paper_ids {
    local $_;
    my $ids_file = shift;
    my $f = IO::File->new("< $ids_file") || die;
    $paper_ids = {};

    while (<$f>) {
        chomp;
        $paper_ids->{$_} = 1;
    }

    $f->close();
    return 0;
}

# If the filename contains the category, we can use that.
# Old filenames look like this:
#        hep-th9211008.pdf
# New filenames look like this:
#        1702.08577.pdf
#
# If it's a new filename, we extract the PDF text and look for the category.
# In old files, the relevant line looks like this:
#              arXiv:hep-th/9211008v1  2 Nov 1992
# In new files:
#              arXiv:1702.08577v1  [hep-th]  27 Feb 2017
#
sub is_pdf_in_category {
	local $_;
	my ($pdf, $category) = @_;

    if (!-x $ghostscript) {
        die "Ghostscript missing: $ghostscript";
    }

    # Only old filenames start with a letter
    my $pdf_basename = $pdf;
    $pdf_basename =~ s/.*\///;

    if ($pdf_basename =~ /^[a-zA-Z]/) {
#print "(OLD FILE) ";
        return ($pdf_basename =~ /^$category\d/);
    }

    if (defined $paper_ids) {
# User the IDs database to search for paper
        my $id = $pdf_basename;
        $id =~ s/\.pdf//;
        return exists $paper_ids->{$id};
    }
    else {
        my $prog = IO::File->new(
                "$ghostscript -sDEVICE=txtwrite -sOutputFile=- " .
                "-dNOPAUSE -dQUIET -dBATCH -dSAFER $pdf |");
        die unless defined $prog;

        while (<$prog>) {
            if (/\barXiv:/) {
                $prog->close();
                return /\b$category\b/;
            }
        }

        $prog->close();
    }

	return 0;
}

1;
