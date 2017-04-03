package Arxiv;

use IO::File;
use DBI;

my $ghostscript = "$ENV{HOME}/gs/gs-921-linux-x86_64";
my $dbfile = '../hep-th.sqlite';
#my $paper_ids;

my $dbh = DBI->connect("dbi:SQLite:dbname=$dbfile","","") || die;

sub is_paper_in_db {
    my $id = shift;
    my $sth = $dbh->prepare("SELECT * FROM papers where id='$id'");
    $sth->execute();
    my $row = $sth->fetch();
    return defined $row;
}

# If the filename contains the category, we can use that.
# Old filenames look like this:
#        hep-th9211008.pdf
# New filenames look like this:
#        1702.08577.pdf
#
# If it's a new filename, we use a metadata database to lookup
# the paper ID.
#
# If that is not available, we extract the PDF text using Ghostscript
# and look for the category.
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

    if (defined $dbh) {
        # User the IDs database to search for paper
        my $id = $pdf_basename;
        $id =~ s/\.pdf//;
        return is_paper_in_db($id);
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
