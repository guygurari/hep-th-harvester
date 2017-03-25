package ArxivPdf;

my $ghostscript = "$ENV{HOME}/gs/gs-921-linux-x86_64";

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
	return 0;
}

