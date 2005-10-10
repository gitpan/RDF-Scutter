package RDF::Scutter;

use strict;
use warnings;
use Carp;

our $VERSION = '0.03';

use base ('LWP::RobotUA');

use RDF::Redland;

sub new {
  my ($that, %params) = @_;
  my $class = ref($that) || $that;
  my $scutterplan = $params{scutterplan};
  croak("No place to start, please give an arrayref with URLs as a 'scutterplan' parameter") unless (ref($scutterplan) eq 'ARRAY');
  delete $params{scutterplan};
  unless ($params{agent}) {
    $params{agent} = $class . '/' . $VERSION;
  }
  croak "Setting an e-mail address using the 'from' parameter is required" unless ($params{from});
  my $self = $class->SUPER::new(%params);
  foreach my $url (@{$scutterplan}) {
    $self->{QUEUE}->{$url} = ''; # Internally, QUEUE holds a hash where the keys are URLs to be visited and values are the URL they were referenced from.
  }
  $self->{VISITED} = {};

  bless($self, $class);
  return $self;
}

sub scutter {
  my ($self, $storage, $maxcount) = @_;
  LWP::Debug::trace('scutter');
  my $model = new RDF::Redland::Model($storage, ""); # $model will contain all we find.
  croak "Failed to create RDF::Redland::Model for storage\n" unless $model;

  my $count = 0;
  while (my ($url, $referer) = each(%{$self->{QUEUE}})) {
    local $SIG{TERM} = sub { $model->sync; };
    next if ($self->{VISITED}->{$url}); # Then, we've been there in this run
#    LWP::Debug::debug('Retrieving ' . $url);
    $count++;
    print STDERR "No: $count, Retrieving $url\n";
    my $response = $self->get($url, 'Referer' => $referer);
    if ($response->is_success) {
      my $fetchtime = $response->header('Date');
      unless ($fetchtime) {
	$fetchtime = localtime;
      }
      $self->{VISITED}->{$url} = 1;  # Been there, done that,
      delete $self->{QUEUE}->{$url}; # one teeshirt is sufficient
      my $uri = new RDF::Redland::URI($url);
      my $context=new RDF::Redland::BlankNode('context'.$count);
      my $parser=new RDF::Redland::Parser;
      unless ($parser) {
	LWP::Debug::debug('Skipping ' . $url);
	LWP::Debug::debug('Could not create parser for MIME type '.$response->header('Content-Type'));
	next;
      }
      my $thisdoc = $parser->parse_string_as_stream($response->decoded_content, $uri);
      unless ($thisdoc) {
	LWP::Debug::debug('Skipping ' . $url);
	LWP::Debug::debug('Parser returned no content.');
	LWP::Debug::conns($response->decoded_content);
	next;
      }

      # Now build a temporary model for this resource
      my $tmpstorage=new RDF::Redland::Storage("memory", "tmpstore", "new='yes',contexts='yes'");
      my $thismodel = new RDF::Redland::Model($tmpstorage, "");
      while($thisdoc && !$thisdoc->end) { # Add the statements to both models
	my $statement=$thisdoc->current;
	$model->add_statement($statement,$context);
	$thismodel->add_statement($statement,$context);

	$thisdoc->next;
      }

      # Now, statements about the contexts
      $model->add_statement($context,  
			    new RDF::Redland::URINode('http://purl.org/net/scutter#source'), 
			    $uri);
      if ($referer) {
	$model->add_statement($context,
			      new RDF::Redland::URINode('http://purl.org/net/scutter#origin'), 
			      new RDF::Redland::URINode($referer));
      }
      my $fetch=new RDF::Redland::BlankNode('fetch'.$count); # It is actually unique to this run, but will have to change later
      $model->add_statement($context,
			    new RDF::Redland::URINode('http://purl.org/net/scutter#fetch'), 
			    $fetch);
      $model->add_statement($fetch,
			    new RDF::Redland::URINode('http://purl.org/dc/elements/1.1/date'), 
			    new RDF::Redland::LiteralNode($fetchtime));
      $model->add_statement($fetch,
			    new RDF::Redland::URINode('http://purl.org/net/scutter#status'), 
			    new RDF::Redland::LiteralNode($response->code));
      $model->add_statement($fetch,
			    new RDF::Redland::URINode('http://purl.org/net/scutter#raw_triple_count'), 
			    new RDF::Redland::LiteralNode($thismodel->size));
      if ($response->header('ETag')) {
	$model->add_statement($fetch,
			      new RDF::Redland::URINode('http://purl.org/net/scutter#etag'), 
			      new RDF::Redland::LiteralNode($response->header('ETag')));
      }
      if ($response->header('Last-Modified')) {
	$model->add_statement($fetch,
			      new RDF::Redland::URINode('http://purl.org/net/scutter#last_modified'), 
			      new RDF::Redland::LiteralNode($response->header('Last-Modified')));
      }


      my $query=new RDF::Redland::Query('SELECT DISTINCT ?doc WHERE { [ <http://www.w3.org/2000/01/rdf-schema#seeAlso> ?doc ] }', undef, undef, "sparql");

      my $results=$query->execute($thismodel);

      while(!$results->finished) {
	for (my $i=0; $i < $results->bindings_count(); $i++) {
	  my $value=$results->binding_value($i);
	  my $foundurl = $value->uri->as_string;
	  unless ($self->{VISITED}->{$foundurl}) {
	    $self->{QUEUE}->{$foundurl} = $url;
	    print STDERR "Adding URL: " . $foundurl ."\n";
	  } else {
	    delete $self->{QUEUE}->{$foundurl};
	    LWP::Debug::debug('Has been visited, so skipping ' . $foundurl);
	  }
	}
	$results->next_result;
      }

#      my $serializer=new RDF::Redland::Serializer("ntriples");
 #     print $serializer->serialize_model_to_string(undef,$model);
 #     $model->sync; # Finally, make sure this is saved to the storage 
      last if ($maxcount == $count);   
    }
  }
  return $model;
}


1;
__END__


=head1 NAME

RDF::Scutter - Perl extension for harvesting distributed RDF resources

=head1 SYNOPSIS

  use RDF::Scutter;
  use RDF::Redland;
  my $scutter = RDF::Scutter->new(scutterplan => ['http://www.kjetil.kjernsmo.net/foaf.rdf','http://my.opera.com/kjetilk/xml/foaf/'], from => 'scutterer@example.invalid');

  my $storage=new RDF::Redland::Storage("hashes", "rdfscutter", "new='yes',hash-type='bdb',dir='/tmp/',contexts='yes'");
  my $model = $scutter->scutter($storage, 30);
  my $serializer=new RDF::Redland::Serializer("ntriples");
  print $serializer->serialize_model_to_string(undef,$model);


=head1 DESCRIPTION

As the name implies, this is an RDF Scutter. A scutter is a web robot
that follows C<seeAlso>-links, retrieves the content it finds at those
URLs, and adds the RDF statements it finds there to its own store of
RDF statements.

This module is an alpha release of such a Scutter. It builds a
L<RDF::Redland::Model>, and can add statements to any
L<RDF::Redland::Storage> that supports contexts. Among Redland
storages, we find file, memory, Berkeley DB, MySQL, etc, but it is not
clear to the author which of these supports contexts, and indeed, it
does seem like the synopsis example doesn't work.

This class inherits from L<LWP::RobotUA>, which again is a
L<LWP::UserAgent> and can therefore use all methods of these classes.

The latter implies it is robot that by default behaves nicely, it
checks C<robots.txt>, and sleeps between connections to make sure it
doesn't overload remote servers.

It implements much of the ScutterVocab at http://rdfweb.org/topic/ScutterVocab

=head1 CAUTION

This is an alpha release, and I haven't tested what it can do if left
unsupervised, and you might want to be careful about finding
out... The example in the Synopsis a complete scutter, but one that
will retrieve only 30 URLs before returning. You could test it by
entering your own URLs (optional) and a valid email address
(mandatory). It'll count and report what it is doing.

=head1 METHODS

=head2 new(scutterplan => ARRAYREF, from => EMAILADDRESS [, any LWP::RobotUA parameters])

This is the constructor of the Scutter. You will have to initialise it
with a C<scutterplan> argument, which is an ARRAYREF containing URLs
pointing to RDF resources. The Scutter will start its traverse of the
web there. You must also set a valid email address in a C<from>, so
that if your scutter goes amok, your victims will know who to blame.

Finally, you may supply any arguments a L<LWP::RobotUA> and
L<LWP::UserAgent> accepts.

=head2 scutter(RDF::Redland::Storage [, MAXURLS]);

This method will launch the Scutter. As first argument, it takes a
L<RDF::Redland::Storage> object. This allows you to store your model
any way Redland supports, and it is very flexible, see its
documentation for details. Optionally, it takes an integer as second
argument, giving the maximum number of URLs to retrieve. This provides
some security against a runaway robot.

It will return a L<RDF::Redland::Model> containing a model with all
statements retrieved from all visited resources.


=head1 BUGS/TODO

There are no known real bugs at the time of this writing, keeping in
mind it is an alpha. If you find any, please use the CPAN Request
Tracker to report them.

I'm in it slightly over my head when I try to add the ScutterVocab
statements. Time will show if I have understood it correctly.

Allthough it uses L<LWP::Debug> to debugging, the author feels it is
somewhat problematic to find the right amount of output from the
module. Subsequent releases are likely to be more quiet than the
present release, however.

For an initial release, heeding C<robots.txt> is actually pretty
groundbreaking. However, a good robot should also make use of HTTP
caching, keywords are Etags, Last-Modified and Expiry. It will be a
focus of upcoming development.

It is not clear how long it would be running, or how it would perform
if set to retrieve as much as it could. Currently, it is a serial
robot, but there exists Perl modules to make parallell robots. If it
is found that a serial robot is too limited, it will necessarily
require attention.

It does not yet give a reason for skipping a resource that fails to be
fetched or parse.

=head1 SEE ALSO

L<RDF::Redland>, L<LWP>.

=head1 SUBVERSION REPOSITORY

This code is maintained in a Subversion repository. You may check out
the trunk using e.g.

  svn checkout http://svn.kjernsmo.net/RDF-Scutter/trunk/ RDF-Scutter


=head1 AUTHOR

Kjetil Kjernsmo, E<lt>kjetilk@cpan.orgE<gt>

=head1 ACKNOWLEDGEMENTS

Many thanks to Dave Beckett for writing the Redland framework and for
helping when the author was confused, and to Dan Brickley for
interesting discussions. Also thanks to the LWP authors for their
excellent library.


=head1 COPYRIGHT AND LICENSE

Copyright (C) 2005 by Kjetil Kjernsmo

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
