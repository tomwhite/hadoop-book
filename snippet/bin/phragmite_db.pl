#!perl
#
# phragmite - extracts potentially syntactically incorrect Java fragments 
#             from compilable source into separate files for inclusion into 
#             DocBook documents
#
# Copyright 2006 Tim Peierls and Brian Goetz
#
# Modifed by Tom White for DocBook. 
#
# Based on a similar program by Ken Arnold, as described by David Holmes.
#
# Usage:
#     phragmite.pl outdir infiles ...
#
# Processes each file in infiles (wildcards are expanded) and puts the
# results in files in output directory outdir.
#
# Text on one line surrounded by /*[*/the text/*]*/ markers is
# replaced by LaTeX listings package markup to put "the text" in boldface.
# (Note we avoid using any XML tags in the comment markup to make escaping easy
# as the target output is DocBook.)
# The token /*...*/ is replaced by "...".
# Any number of spaces surrounded by /* and */ is deleted; this is to make
# it easier to align lines with differing amounts of /*xxx*/ markup.
#
# The token /*public*/ is replaced by plain public; this is to allow
# more than one "public" class in the same source file. Same thing with
# /*private*/
#
# A line in an input file marked with 
#
#   // vv FragmentName optionalText
#
# is treated as an instruction to add any following lines to a fragment 
# named "FragmentName". If optionalText is not empty, it will be added 
# to the fragment at this point.
#
# A line in an input file marked with 
#
#   // ^^ FragmentName optionalText
#
# is treated as an instruction to stop adding lines to the fragment 
# named "FragmentName". If optionalText is not empty, it will be added 
# to the fragment at this point.
#
# A line in an input file marked with 
#
#   // xx FragmentName optionalText
#
# is treated as an instruction to add a line to the fragment
# named "FragmentName" with contents given by optionalText. 
# If optionalText is empty, an empty line is added.
#
# A line marked with
#
#   // hh FragmentName text
#
# is treated as "header" information, and is inserted in a floating
# listings before the listing is opened.
#
# A line in an input file marked with any of these forms
#
#   // cc FragmentName text
#   // !! FragmentName text
#   // ?? FragmentName text
#   // == FragmentName text
#
# is treated as a caption for the fragment named "FragmentName". 
# The cc form (the normal case) marks the fragment as neutral.
# The !! form marks it as a good example and the ?? form marks
# it as a bad example. In all of these cases, the fragment is
# floated in the page, but the == form marks it as a non-floating 
# neutral example. 
#
# If multiple captions are given, the last encountered caption is used.
# If any of the multiple captions are marked with !! or ??, the fragment 
# will be treated as a good example or a bad example, respectively. If
# it marked with both !! and ??, a warning is generated and the fragment
# is treated as a bad example.
#
# A line in an input file marked with
#
#   // -- FragmentName text
#
# is treated as additional listing options for the fragment named "FragmentName".
#
# If an input file has no fragment markup, its entire contents will be 
# used as a fragment, taking the name from the name of the file after 
# stripping the directory and .java extension.
#
# Fragments are emitted as separate files in the output directory. If 
# multiple input files contribute to a fragment, the contents of the 
# fragment will be in the order that the input files appear on the 
# command line, subject to wildcard expansion.
#
# The resulting files depend on the existence of listing environments
# Java, JavaNoFloat, GoodExample, and CounterExample, and the use of 
# backtick ` as a listing escape character.
#
# Lines of the form 
#
#   // ((
#
# and
#
#   // ))
#
# are currently ignored, but should be used to delimit lines to be discarded
# when phragmite is extended to strip markup and produce clean files.
#

#
# Maximum safe length of lines in listings (includes 2 space pad).
#
use constant MAXLINE => 83;

#
# Whether to print verbose information about processing.
#
use constant VERBOSE => 1;


#
# Make sure output directory exists and is a writable directory.
#
my $outdir = shift @ARGV;
$outdir =~ s{\\}{/}g; # normalize to forward slashes
-e $outdir or mkdir $outdir;
(-d $outdir && -w _) or die "can't open $outdir as writable directory";

#
# Fragments keyed by name, the values are the contents of each fragment.
# Hashes to hold captions, extra options, which examples are good, bad,
# or not to be floated.
#
my (%fragments, %fileNames, %captions, %options, %good, %bad, %nofloat);

#
# Process each file, expanding wildcards and normalizing to forward slashes
#
for my $file (map { s{\\}{/}g; glob $_ } @ARGV) {
    #
    # Ignore unreadable files
    #
    if (!open FILE, "<", $file) { warn "can't open $file for reading"; next; }
    
    my $fileName = $file;
    $fileName =~ s{.*/}{};   # strip directory

    my $targetFileName = "$outdir/$fileName";
    $targetFileName =~ s/\.java$/\.xml/;

    if ((stat($file))[9] < (stat($targetFileName))[9]) {
      print STDERR "$targetFileName is up to date\n" if VERBOSE;
      next;
    }
    
    print STDERR "Extracting fragments from $fileName\n" if VERBOSE;
    
    my %active;
    my $entireFile;
    my $lineNum = 0;   # first line is numbered 1
    while (<FILE>) {
        ++$lineNum;
        
        # 
        # Replace bold markup comments with actual LaTeX bold markup
        # and dots markup with actual LaTeX dots markup.
        #
        my $line = $_;
        $line =~ s!&!&amp;!g;                         # escape XML &
        $line =~ s!<!&lt;!g;                          # escape XML <
        $line =~ s!>!&gt;!g;                          # escape XML >
        $line =~ s!/\*\*\s*(\S+(?:\s+\S+)*)\s*\*\*/!/\* \`\{$1\}\` \*/!g;
        $line =~ s!/\*\[\*/!<emphasis role="bold">!g; # /*[*/
        $line =~ s!/\*]\*/!</emphasis>!g;             # /*]*/
        $line =~ s!/\*\.\.\.\*/!...!g;                # /*...*/
        $line =~ s!/\*\s+\*/!!g;                      # /*   */
        $line =~ s!/\*public\*/!public!;              # /*public*/
        $line =~ s!/\*private\*/!private!;            # /*private*/
        
        if ($line =~ m{^ \s* // \s* (vv+|\^\^+|xx+|cc+(?:\[.*\])?|hh+|!!+|\?\?+(?:\[.*\])?|==+|--+) \s+ (\S*) \s? (.*) $ }x) {
            # 
            # This is a phragmite markup line: parse operation, fragment name, and remainder
            #
            my $op = $1;                             # operation (vv, ^^, xx, cc, !!, ??, ==, --
            my $key = $2;                            # fragment name
            my $rest = $3;                           # optional remainder of line
            $rest =~ s/[\n\r]+$//;                   # strip trailing \n and \r
            $fileNames{$key} = $fileName;            # keep track of file contributing to this fragment
                                                     # Only remembers last file seen.
            
            if ($op =~ /c/) {    # caption
                warn "line $lineNum: missing caption for $key\n" unless $rest;
                $captions{$key} = $rest;
                if ($op =~ /cc\[(.*)\]/) {
                    $vertical{$key} = $1;
                }
                next;
            }
            if ($op =~ /!/) {    # good example caption
                $captions{$key} = $rest;
                $good{$key} = $rest;
                next;
            }
            if ($op =~ /\?/) {   # bad example caption
                $captions{$key} = $rest;
                $bad{$key} = $rest;
                if ($op =~ /\?\?\[(.*)\]/) {
                    $vertical{$key} = $1;
                }
                next;
            }
            if ($op =~ /=/) {   # non-floating example caption
                $captions{$key} = $rest;
                $nofloat{$key} = $rest;
                next;
            }
            if ($op =~ /-/) {    # extra options
                warn "line $lineNum: missing options for $key\n" unless $rest;
                $options{$key} = $rest;
                next;
            }
            if ($op =~ /x/) {    # one-off insertion
                $line =~ s{//.*}{$rest};                # replace markup comment with trailing text
                $fragments{$key} .= $line;              # append line to fragment
                $active{$key} = 0 unless $active{$key}; # record that we saw this fragment
                next;
            }
            if ($op =~ /h/) {    # header insertion
                $line =~ s{//.*}{$rest};                # replace markup comment with trailing text
                $header{$key} .= $line;                 # append line to header
                $active{$key} = 0 unless $active{$key}; # record that we saw this fragment
                next;
            }
            my $active;
            $op =~ /v/  and $active = $lineNum;  # opening fragment, save line number
            $op =~ /\^/ and $active = 0;         # closing fragment
            unless (defined $active) {
                warn "line $lineNum: unknown operator '$op'\n";
                next;
            }
            $active{$key} && $active and
                warn "line $lineNum: opening fragment $key that is already open.\n";
            !$active{$key} && !$active and
                warn "line $lineNum: closing fragment $key that was not open.\n";
            $active{$key} = $active;
            if ($rest) {
                $line =~ s{//.*}{$rest};    # replace markup comment with trailing text
                $fragments{$key} .= $line;  # append line to fragment
            }
        } else {
            # 
            # This is a normal line, add it to entire file fragment and
            # to any active fragments.
            #
            $entireFile .= $line;
            for my $key (keys %active) {
                $fragments{$key} .= $line if $active{$key};
            }
        }
    }
    
    my @keys = keys %active;
    if (@keys) {
        #
        # Make sure all active fragments are closed, issuing warning if not.
        #
        for my $key (@keys) {
            my $active = $active{$key};
            $active and
                warn "line $active: fragment $key never closed.\n";
        }
    } else {
        #
        # No fragments in this file, so treat entire file as fragment with
        # name taken from file name, stripping directory and .java extension.
        #
        warn "Entire file $fileName used.\n" if VERBOSE;
        $fileName =~ s/\.java$//;
        $fragments{$fileName} = $entireFile;
        $nofloat{$fileName} = 1 if ! $captions{$fileName};
    }
}

#
# Emit fragments
#
print STDERR "\nPutting results in $outdir...\n\n" if VERBOSE;
for my $key (sort keys %fragments) {
    my $fragment = $fragments{$key};
    if ($fragment =~ m/^\s*$/s) {
        warn "skipping blank fragment '$key'\n";
        next;
    }

    my $outfile = "$outdir/$key.xml";
    if (!open OUT, ">", $outfile) {
        warn "can't open $outfile for writing";
        next;
    }
    
    print STDERR "Generating fragment $key\n" if VERBOSE;
    
    lengthCheck($key, $fragment);
    
    my $caption = $captions{$key};
    my $options = $options{$key};
    my $good    = exists $good{$key} ? 1 : 0;
    my $bad     = exists $bad{$key} ? 1 : 0;
    my $nofloat = exists $nofloat{$key} ? 1 : 0;
    my $vertical = exists $vertical{$key} ? $vertical{$key} : .3;
    if ($good + $bad + $nofloat > 1) {
        warn "fragment $key should be one of !!, ??, or ==\n";
        undef $good;
    }

    # Just print out the <programlisting> element, since <example> wrappers can be
    # slightly different in the manuscript due to whitespace differences.

    #print OUT "<!-- Generated from ", $fileNames{$key}, " -->\n";
#    if ($good) {
#        print OUT "<example id=\"$key\">\n";
#        print OUT $header{$key};
#        print OUT "  <title>$caption</title>\n";
#        print OUT "  <programlisting";
#        print OUT " $options" if $options;
#        print OUT " format=\"linespecific\">$fragment</programlisting>\n";
#        print OUT "</example>\n";
#    } elsif ($bad) {
#        print OUT "<example id=\"$key\">\n";
#        print OUT $header{$key};
#        print OUT "  <title>$caption</title>\n";
#        print OUT "  <programlisting";
#        print OUT " $options" if $options;
#        print OUT " format=\"linespecific\">$fragment</programlisting>\n";
#        print OUT "</example>\n";
#    } elsif ($nofloat) {
        print OUT "<programlisting";
        print OUT " $options" if $options;
        print OUT " format=\"linespecific\">$fragment</programlisting>\n"; 
#    } else { # Hmm
#        print OUT "<example id=\"$key\">\n";
#        print OUT $header{$key};
#        print OUT "  <title>$caption</title>\n";
#        print OUT "  <programlisting";
#        print OUT " $options" if $options;
#        print OUT " format=\"linespecific\">$fragment</programlisting>\n";
#        print OUT "</example>\n";
#    }
#
    close OUT;
}

sub leftShift {
    my $key = shift;
    local $_ = shift;
    
    #
    # Convert space-only lines to empty lines and
    # get count of non-blank lines.
    #
    my $count = s/^//mg;
    $count -= s/^\s*$//mg;      
    warn "empty fragment $key" unless $count;
    
    #
    # Shift every line left two spaces until not every non-blank line 
    # changes, then back up to previous value.
    #
    my ($prev, $ncount);
    do {  $prev = $_;  $ncount = s/^  //mg; } until $ncount < $count;
    $_ = $prev;
    
    #
    # Add two spaces to every line (because Java listings style has gobble=2)
    #
    s/^/  /mg; 
    
    return $_;
}

sub lengthCheck {
    my $key = shift;
    my $frag = shift;
    $frag =~ s!(&lt;|&gt;|&amp;)!1!g;
    $frag =~ s!<emphasis role="bold">!!g;
    $frag =~ s!</emphasis>!!g;
    my $maxline = MAXLINE + 1;
    my @longLines = $frag =~ m/.{$maxline}/g;
    if (@longLines) {
        my $badlines = join("\n", @longLines);
        my $nbadlines = @longLines;
        my $s = $nbadlines > 1 ? "s" : "";
        warn "$nbadlines line$s longer than ".MAXLINE." characters in fragment $key:\n$badlines\n";
    }
}


__END__
