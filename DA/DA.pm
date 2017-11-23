package DBIx::DA;

BEGIN {
  $DBIx::DA::VERSION = "0.01";
}
# use lib qw(C:\CPAN\DBIx-DA-01\lib\DBIx\DA
            # C:\johns\Dropbox\Code_Base\Orignal\lib
            # C:\johns\Dropbox\Code_Base\DBIx-DA-01\lib
            # );
use base qw(Orignal Exporter);
use Carp();
use warnings;
use strict;
use Data::Dumper;
use DBIx::DA::Constants::DA;
DBIx::DA->attributes({SCALARS =>[qw(
                                    _dbh
                                    _operation
                                    _container
                                    _initialized
                                    name
                                    cache_connection
                                    can_modify
                                    can_global_delete
                                    can_global_update
                                    max_num_of_records
                                    last_cache_refresh
                                    num_hours_to_cache
                                    success
                                    connect_attr
                                    data_source
                                    username
                                    password
                                    PrintError
                                    PrintWarn
                                   
                                    
                                    )],
                         HASHES          =>[],
                         #ORDERED_HASHES  =>[qw(field)],
                         ARRAYS  =>[qw(transactions
                                        results)]#[qw( tables static_criteria dynamic_criteria order_bys)]
                         });
                         

sub _connect {
   my $self = shift;
   return $self->_dbh()
      if ($self->_dbh());
   my $dbh;
   if ($self->cache_connection()){
      $dbh = DBI->connect_cached($self->data_source,$self->username,$self->password,$self->connect_attr);
   }
   else {
      $dbh = DBI->connect($self->data_source,$self->username,$self->password,$self->connect_attr);
   }
   $self->_dbh($dbh);
   return $dbh;
}
sub create {
   my $self=shift;
   my ($dbh,$container) = @_;
   die "error: ".ref($self).", Attempt to do a 'create' operation when can_modify is false!"
      unless ($self->can_modify());
   $self->_operation(Constants::DA::CREATE);
   return $self->_execute($dbh,$container);
}
sub retrieve {
   my $self=shift;
   my ($dbh,$container,$opt) = @_;
   $self->_operation(Constants::DA::RETRIEVE);
   return $self->_execute($dbh,$container,$opt);
}
sub update {
   my $self=shift;
   my ($dbh,$container) = @_;
   die "error: ".ref($self).", Attempt to do an update operation when can_modify is false!"
      unless ($self->can_modify());
   $self->_operation(Constants::DA::UPDATE);
   return $self->_execute($dbh,$container);
}
sub delete {
   my $self=shift;
   my ($dbh,$container) = @_;
   die "error: ".ref($self).", Attempt to do a delete operation when can_modify is false!"
      unless ($self->can_modify());
   $self->_operation(Constants::DA::DELETE);
   return $self->_execute($dbh,$container);
}
sub validate_connect_attr {
   my $self = shift;
   my ($opt) = @_;
 	
   die("ERROR: DBIx::DA::connect_attr, sssssmust be  a 'HASH' ref!")
     if (ref($opt) ne 'HASH');
}

sub validate_transactions {
   my $self = shift;
    my ($opt) = @_;
   #warn("validate_transactions=".ref($self)."\n");
   if (ref($opt) eq 'ARRAY'){
       foreach my $sql (@{$opt}){
          if (ref($sql) ne 'DBIx::DA::SQL'){
             die("ERROR: DBIx::DA::SQL_transactions, must be  a 'DBIx::DA::SQL' object.");
          }
       }
   }
   elsif( ref($opt) ne 'DBIx::DA::SQL'){
      die("ERROR: DBIx::DA::SQL_transactions, must be 'DBIx::DA::SQL' object.");
   }
   return 1;
}
sub execute_transactions {
   my $self = shift;
   my $dbh = DBI->connect('dbi:Oracle:XE','hr','hr',);
   my %atttrib =(AutoCommit=>$dbh->{AutoCommit},
                 RaiseError=>$dbh->{RaiseError});
   $dbh->{AutoCommit} = 0;
   $dbh->{RaiseError} = 1;
   foreach my $transaction ($self->transactions()){
      eval{
         $transaction->execute($dbh);
      };
      if ($@){
         $dbh->rollback();
         my $error_msg = "ERROR: DBIx::DA::execute_transaction, All transactions rolled back because of $@.";
         if ($self->PrintWarn()){
             warn($error_msg);
             return 0;
         }
         else {
            die($error_msg);
         }
      }
   }
   $dbh->commit();
   $self->_reset_connect_attributes($dbh,\%atttrib);
   return 1;
}
sub _reset_connect_attributes {
   my $self = shift;
   my ($dbh,$attribute) = @_;
   foreach my $att (keys(%{$attribute})){
      $dbh->{$att}=$attribute->{$att};
   }
}


sub new {

   my $self = shift;
   #warn("DA new ".$self->_initialized()."/n");
  $self->initialize()
     unless($self->_initialized());
   if (ref(@_) eq "HASH"){
     $self = $self->SUPER::new(@_);
   }
   else {
     $self = $self->SUPER::new();           
   }
   
   $self->reset()
      if($self->_initialized());
   
   return $self;        
        
}

sub expire_cache {
   my $self  = shift;
   my $new_date = "";
   $self->last_cache_refresh($new_date);
}
sub clear_dynamic_criteria{
   my $self = shift;
   $self->expire_cache(undef);
   $self->dynamic_criteria({});
}
sub add_dynamic_criterion {
   my $self = shift;
   my ($field,$operation, $constraint) = @_;
   #selft make sure the above are a name, $int and an object
   $self->expire_cache();
   my %criterion=(FIELD     =>$field,
                  OPERATION =>$operation,
                  CONSTRAINT=>$constraint);
   $self->push_dynamic_criteria(\%criterion);
}


=head1 AUTHOR
John Scoles, C<< <byterock at hotmail.com> >>
=head1 BUGS
Please report any bugs or feature requests to C<bug-dbix-da at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-DA>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.
=head1 SUPPORT
You can find documentation for this module with the perldoc command.
    perldoc DBIx::DA
You can also look for information at:
=over 4
=item * RT: CPAN's request tracker
L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-DA>
=item * AnnoCPAN: Annotated CPAN documentation
L<http://annocpan.org/dist/DBIx-DA>
=item * CPAN Ratings
L<http://cpanratings.perl.org/d/DBIx-DA>
=item * Search CPAN
L<http://search.cpan.org/dist/DBIx-DA/>
=back
=head1 ACKNOWLEDGEMENTS
=head1 LICENSE AND COPYRIGHT
Copyright 2011 John Scoles.
This program is released under the following license: open_source
=cut
1; # End of DBIx::DA
