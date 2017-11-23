package DBIx::DA::SQL;
BEGIN {
$DBIx::DA::SQL::VERSION = "0.01";
}
 use lib qw( D:\Perl64\site\lib);

#use lib qw(C:\Users\John Scoles\Dropbox\Code_Base\chemstore_perl_05_19\Orignal\Orignal\lib\DA);
use base qw(DBIx::DA Exporter);
use DBI;
use Carp();
use Data::Dumper;
use warnings;
use strict;
use DBIx::DA::Constants::SQL;
use DBIx::DA::Constants::DA;
DBIx::DA::SQL->attributes({SCALARS =>[qw(initialized
                                         rows_effected
                                         static_sql
                                         table
                                         distinct
                                         group_by
                                         where
                                         having
                                         use_named_params
                                         returning
                                         type
                                         use_field_alias
                                          )],
                           ARRAYS  =>[qw(fields
                                         joins
                                         dynamic_joins
                                         hierarchical_joins
                                         order_bys
                                         conditions
                                         dynamic_conditions
                                         _params
                                         only_fields
                                          )],
                           HASHES  =>[qw(_field_named
                                         )],
});

sub initialize {
   my $self = shift;
   #warn("SQL initalize/n");
   $self->fields([]);
   $self->joins([]);
   $self->_field_named({});
   $self->reset();
   $self->_initialized(1);
}
   
sub reset {
   my $self = shift;
   #warn("SQL reset/n");
   $self->conditions([]);
   $self->dynamic_joins([]);
   $self->dynamic_conditions([]);
   $self->order_bys([]);
   $self->only_fields([]);
   $self->expire_cache();
}

sub _identity_keys {
   my $self = shift;
   my @keys = ();
   foreach my $field ($self->fields){
          #warn(Dumper($field));
      if ($field->is_identity() || $field->is_unique() ){
          my $name = $field->name();
          if ($field->alias()) {
             $name = $field->alias();
           }
           push(@keys,$name);
      }
   }
   return @keys;
}
sub add_left_outer_join {
    my $self = shift;
    my ($opt) =@_;
    
    ref($opt) eq 'HASH' ||  die("ERROR: DBIx::DA::SQL::inner_join, must be an HASH Ref  of 'Join Attributess'.");
  
    #$param = $self->field_named($opt->{'to_field'});
    my $on_param = $self->field_named($opt->{'on_field'});
    
    $on_param = DBIx::DA::Field->new({name       =>$opt->{on_field},
                                      table_name =>$opt->{on_table}})
    if ($opt->{on_table});
                             
    
    my $to_param = DBIx::DA::Field->new({name       =>$opt->{to_field},
                                         table_name =>$opt->{table_alias}?$opt->{table_alias}:$opt->{to_table}});
    #  unless($param);  
                                  
    my $operator = $opt->{operator}|| '=';
      #warn("add_inner_join=".$self->field_named($opt->{'on_field'})); 
    $self->add_join({table_name =>$opt->{to_table},
                     table_alias=>$opt->{table_alias},
                     type       =>Constants::SQL::LEFT_OUTER,
                     conditions =>{field    =>$on_param,
                                   operator =>$operator,
                                   param    =>$to_param }});
        
 }
sub add_left_join {
    my $self = shift;
    my ($opt) =@_;
    
    ref($opt) eq 'HASH' ||  die("ERROR: DBIx::DA::SQL::inner_join, must be an HASH Ref  of 'Join Attributess'.");
  
    #$param = $self->field_named($opt->{'to_field'});
    my $on_param = $self->field_named($opt->{'on_field'});
    
    $on_param = DBIx::DA::Field->new({name       =>$opt->{on_field},
                                      table_name =>$opt->{on_table},
                                      })
       if ($opt->{on_table});
                             
   my $to_param = DBIx::DA::Field->new({name       =>$opt->{to_field},
                                         table_name =>$opt->{table_alias}?$opt->{table_alias}:$opt->{to_table}});
    #  unless($param);  
                                  
    my $operator = $opt->{operator}|| '=';
      #warn("add_inner_join=".$self->field_named($opt->{'on_field'})); 
    $self->add_join({table_name =>$opt->{to_table},
                     table_alias=>$opt->{table_alias},
                     type       =>Constants::SQL::LEFT,
                     conditions =>{field    =>$on_param,
                                   operator =>$operator,
                                   param    =>$to_param }});
        
 }
sub add_left_inner_join {
    my $self = shift;
    my ($opt) =@_;
    
    
    ref($opt) eq 'HASH' ||  die("ERROR: DBIx::DA::SQL::inner_join, must be an HASH Ref  of 'Join Attributess'.");
  
   
    my $operator = $opt->{operator}|| '=';
      #warn("add_inner_join=".$self->field_named($opt->{'on_field'})); 
    $self->add_join({table_name =>$opt->{to_table},
                     table_alias=>$opt->{table_alias},
                     type       =>Constants::SQL::LEFT_INNER,
                     conditions =>{field    =>$self->field_named($opt->{'on_field'}),
                                   operator =>$operator,
                                   param    =>DBIx::DA::Field->new({name       =>$opt->{to_field},
                                                                    table_name =>exists($opt->{table_alias})?$opt->{table_alias}:$opt->{to_table}})}});
        
 }
 sub add_static_inner_join {
    my $self  = shift;
    my ($opt) = @_;
    
    ref($opt) eq 'HASH' ||  die("ERROR: DBIx::DA::SQL::inner_join, must be an HASH Ref  of 'Join Attributess'.");
  
     
                                  
    my $operator = $opt->{operator}|| '=';
    #  warn("add_inner_join=".ref($self).",".$self->field_named($opt->{'on_field'})); 
    $self->add_static_join({table_name =>$opt->{to_table},
                           table_alias=>$opt->{table_alias},
                     type       =>Constants::SQL::INNER,
                     conditions =>{field    =>$self->field_named($opt->{'on_field'}),
                                   operator =>$operator,
                                   param    =>DBIx::DA::Field->new({name       =>$opt->{to_field},
                                                                    table_name =>exists($opt->{table_alias})?$opt->{table_alias}:$opt->{to_table}})}});
        
 }
sub add_static_left_join {
    my $self  = shift;
    my ($opt) = @_;
    
    ref($opt) eq 'HASH' ||  die("ERROR: DBIx::DA::SQL::inner_join, must be an HASH Ref  of 'Join Attributess'.");
  
     
                                  
    my $operator = $opt->{operator}|| '=';
    #  warn("add_inner_join=".ref($self).",".$self->field_named($opt->{'on_field'})); 
    $self->add_static_join({table_name =>$opt->{to_table},
                           table_alias=>$opt->{table_alias},
                     type       =>Constants::SQL::LEFT,
                     conditions =>{field    =>$self->field_named($opt->{'on_field'}),
                                   operator =>$operator,
                                   param    =>DBIx::DA::Field->new({name       =>$opt->{to_field},
                                                                    table_name =>exists($opt->{table_alias})?$opt->{table_alias}:$opt->{to_table}})}});
        
 }
sub add_inner_join {
    my $self  = shift;
    my ($opt) = @_;
    
    ref($opt) eq 'HASH' ||  die("ERROR: DBIx::DA::SQL::inner_join, must be an HASH Ref  of 'Join Attributess'.");
  
     
                                  
    my $operator = $opt->{operator}|| '=';
    # warn(Dumper($opt));
      # warn("add_inner_join= self=".ref($self).", field=".$self->field_named($opt->{'on_field'})); 
    $self->add_join({table_name =>$opt->{to_table},
                     type       =>Constants::SQL::INNER,
                     conditions =>{field    =>$self->field_named($opt->{'on_field'}),
                                   operator =>$operator,
                                   param    =>DBIx::DA::Field->new({name       =>$opt->{to_field},
                                                                    table_name =>$opt->{to_table}})}});
        
 }

sub add_right_inner_join {
    
    my $self = shift;
    my ($opt) =@_;
    
    ref($opt) eq 'HASH' ||  die("ERROR: DBIx::DA::SQL::inner_join, must be an HASH Ref  of 'Join Attributess'.");
  
      my $param = $self->field_named($opt->{'to_field'});
    $param = DBIx::DA::Field->new({name       =>$opt->{to_field},
                                   table_name =>$opt->{to_table}})
      unless($param);  
                     
                                  
    my $operator = $opt->{operator}|| '=';
      #warn("add_inner_join=".$self->field_named($opt->{'on_field'})); 
    $self->add_join({table_name =>$opt->{to_table},
                     type       =>Constants::SQL::RIGHT_INNER,
                     conditions =>{field    =>$self->field_named($opt->{'on_field'}),
                                   operator =>$operator,
                                   param    =>$param}});
        
}

sub add_right_join {
    my $self = shift;
    my ($opt) =@_;
    
    ref($opt) eq 'HASH' ||  die("ERROR: DBIx::DA::SQL::inner_join, must be an HASH Ref  of 'Join Attributess'.");
  
   #   my $param = $self->field_named($opt->{'to_field'});
    my $param = DBIx::DA::Field->new({name       =>$opt->{to_field},
                                   table_name =>$opt->{to_table}});
      #unless($param);  
                     
                                  
    my $operator = $opt->{operator}|| '=';
      #warn("add_inner_join=".$self->field_named($opt->{'on_field'})); 
    $self->add_join({table_name =>$opt->{to_table},
                     type       =>Constants::SQL::RIGHT,
                     conditions =>{field    =>$self->field_named($opt->{'on_field'}),
                                   operator =>$operator,
                                   param    =>$param}});
        
 }

sub add_table {
    my $self  = shift;
    my ($opt) = @_;    
 
    $self->table(DBIx::DA::Table->new($opt));
        
 }
 
 sub add_fields {
    my $self  = shift;
    my ($opt) = @_; 
    ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::SQL::add_fields, must be an ARRAY of 'Field Attributess'.");
    foreach my $field (@{$opt}){
       $field->{table_name} = $field->{table};
      
        # $field->{table_name} = $self->table()->name()
           # unless ($field->{table_name});
       #$field->{alias} = $field->{table_name}."_".$field->{name}
       #  if ($self->table->name() ne $field->{table_name});
    
       $self->push_fields(DBIx::DA::Field->new($field));
    }
    return;    
        
}
 
sub _execute {
   my $self = shift;
   my ($dbh,$container,$opts)=@_;
   my $exe_array = 0;
   $self->results([]);
   $self->_params([]);
  # warn("\n\n\n ".$self." Execute start ");
  # warn(" dbh=".$dbh.", continer=".$container."\n");
    # # 
   #   warn("I Start with >dynamic_condition = ".Dumper($self->dynamic_conditions));
    # warn("I Start with condition = ".Dumper($self->conditions));
    # warn("I Start with joins = ".Dumper($self->joins));
   # warn("I Start with dynamic_joins = ".Dumper($self->dynamic_joins));
   # 
   
   $dbh = $self->_connect()
     unless($dbh);
 
   my $sql;
 
   if ($self->static_sql()){
      $sql = $self->static();
   }
   elsif($self->_operation eq Constants::DA::CREATE){
      $sql = $self->_insert($container);
   }
   elsif($self->_operation eq Constants::DA::UPDATE){
      $sql = $self->_update($container);
   }
   elsif($self->_operation eq Constants::DA::DELETE){
      $sql = $self->_delete();
   }
   else{
      $sql = $self->_select();
   }
   #warn("my SQL qa =".$sql."\n");
  
   my $sth;
   #warn("my $dbh =".$dbh."\n");
  
   eval{
   	
   	$sth=$dbh->prepare($sql);
   };
   if ($@){
   	warn("error=".$@);
   }
   my @params;# = $self->_params();
   
   
   foreach my $param ($self->_params()){
      my $value = $param->value();
      
      if ($value and $value->isa("DBIx::DA::SQL")){
         foreach my $nested ($value->_params){ #this needs to recurse
            push(@params,$nested);
         }   
      }
      else{
         push(@params,$param);
      }
   }
   my $param_count = scalar(@params);
  
   for(my $count=1;$count <= $param_count; $count++){
      my $param = shift(@params);
      my $value = $param->value();
      
      my %type  = ();
    
      if ($param->type){
        $type{type}=$param->type();
      }
      #warn("bind value=$value \n");
     
      # 
      if (ref($value) eq 'ARRAY') {
        $sth->bind_param_array($count, $value, %type);
        $exe_array = 1;
      #   warn("bind value=".ref($value)."\n");
      }
      else {
        if ($self->use_named_params){
          $sth->bind_param(":p_".$param->name(), $value, %type);
        }
        else{
          $sth->bind_param($count, $value, %type);
        }
      }
   }
   my @returns =undef;
   
   if (($self->returning()) and ($self->_operation() ne Constants::DA::RETRIEVE)){
      my @params = $self->returning()->params();
      my $return_count = scalar(@params);
      @returns=(1..$return_count);
      for (my $count=1;$count<=$return_count; $count++){
         my $param = shift(@params);
         my %type  = ();
         if ($param->type){
            $type{type}=$param->type();
         }
         if ($self->use_named_params){
            $sth->bind_param_inout(":p_".$param->name(),\$returns[$count-1],'100', %type);
         }
         else{
            $sth->bind_param_inout($count+$param_count,\$returns[$count-1],'100', %type);
         }
         
      }
   }
   if ($self->_operation() eq Constants::DA::RETRIEVE){
     # warn("JPS exe");
      $sth->execute();
     # warn("JPS exe2 container=".ref($container));
      
      $container = [] 
        if (!$container);
   
      if (ref($container) eq 'ARRAY'){
         my $results = $sth->fetchall_arrayref();
       #  push(@{$container},@{$results});
         $self->results($results); 
      }
      elsif(ref($container) eq "HASH"  or $container->isa("UNIVERSAL")){
         my @key_fields =  $self->_identity_keys()
           ;#(ref $key_field) ? @$key_field : ($key_field);
         if(!scalar(@key_fields)){
            die "error: DBIx::DA:::SQL->execute attempt to use a HASH Ref as container without a DBIx::DA::Field without an is_identity attribute!";
         }
         my $hash_key_name = $sth->{FetchHashKeyName} || 'NAME_lc';
         if ($hash_key_name eq 'NAME'  or $hash_key_name eq 'NAME_uc' ){
           @key_fields=map(uc($_),@key_fields);
         }
         else {
           @key_fields=map(lc($_),@key_fields);
         }
         my $names_hash = $sth->FETCH("${hash_key_name}_hash");
         my @key_indexes;
         my $num_of_fields = $sth->FETCH('NUM_OF_FIELDS');
         foreach (@key_fields) {
                 
           my $index = $names_hash->{$_};  # perl index not column
           $index = $_ - 1 if !defined $index && DBI::looks_like_number($_) && $_>=1 && $_ <= $num_of_fields;
           return $sth->set_err($DBI::stderr, "Field '$_' does not exist (not one of @{[keys %$names_hash]})")
                unless defined $index;
           push @key_indexes, $index;
         }
         my $NAME = $sth->FETCH($hash_key_name);
         my @row = (undef) x $num_of_fields;
         $sth->bind_columns(\(@row));
      
         while ($sth->fetch) {
             
             if (ref($container) eq "HASH"){
               my $ref = $container; #();#$rows;
               $ref = $ref->{$row[$_]} ||= {} for @key_indexes;
               @{$ref}{@$NAME} = @row;  
               $self->push_results($ref);
             }
             else {
                my $new_item =  $container->new(); 
                #warn(ref($container));  
                foreach my $key (keys(%{$names_hash})) {
                       
                   $new_item->$key($row[$names_hash->{$key}])
                     if ($new_item->can($key));
                   # #$ref = $ref->$row[$_]} ||= {} for @key_indexes; 
                }
               
               $new_item = {%$new_item}
                   if ($opts->{CLASS_AS_HASH});
               
               $self->push_results($new_item);
                
             }
            
         }
        
       }
   }
   else {
      if ($exe_array){
        #warn("exe array here\n");
        my @tuple_status;
        
        my $tuples = $sth->execute_array({ArrayTupleStatus => \@tuple_status});

        $self->rows_effected(scalar($tuples));
       
      }
      else {
        my $rows_effected = $sth->execute();
        $self->rows_effected($rows_effected);
        if (@returns){
          $self->push_results(\@returns);
        }
        
      }
      $dbh->commit();
    }
    
      $dbh->{dbd_verbose}=0;

   #warn("end SQL iam a ".ref($self));
   # warn("In I  have ".ref($self)." condition = ".scalar($self->dynamic_conditions));
    # $self->dynamic_joins([]);
   # $self->dynamic_conditions([]);
   # warn("Out I  have condition = ".$self->dynamic_conditions);
   # 
}


sub empty_fields{
   my $self = shift;
    $self->fields([]);
   $self->_field_named({});        
        
 }
sub empty  {
   my $self = shift;
   
   $self->distinct("");
   $self->table(undef);
   $self->fields([]);
   $self->_field_named({});
   $self->joins([]);
   $self->conditions([]);
   
   $self->group_by(undef);
   $self->having(undef);
   $self->order_bys([]);
}
sub _field_exists {
   my $self    = shift;
   my ($field) = @_;
   
   my $current_index = $self->values__field_named();
   
   my $field_name    = $field->name();
   
   $field_name       = $field->alias()
      if ($field->alias());
 
   #my $table_name    = $field->table_name();
  
   # if ($table){
     # if ($$table->alias()){
       # $table_name = $$table->alias()
     # }
     # else {
       # $table_name = $$table->name();
     # }
     # $table_name.='.';
   # }
   # $field_name= $table_name.".".$field_name;
  #   #warn("table_name=".$table_name."\n");
   #warn("field_name=".$field_name."\n");
   if($field->alias()) {
     if($self->exists__field_named($field_name)){
        die("ERROR:".($self).", field with alias=".$field_name." is already defined!");
     }
   }
   else{
     if($self->exists__field_named($field_name)){
       die("ERROR:".($self).", field with name=".$field_name." is already defined! Try using an alias!");
     }
   }
   $self->_field_named({$field_name=>$current_index});
}
sub field_value{
   my $self= shift;
   my ($field_name) = @_;
    #warn("1 in field_named=".$field_name."\n");
   # my $table_name   = "";
   # $table_name      = $self->table()->name()
      # if ($self->table->name());
   # $table_name      = $self->table()->alias()
      # if ($self->table->alias());
      # 
   # $field_name= $table_name.".".$field_name
      # if ($table_name);
   # warn("field_named=".$field_name."\n");
   if($self->exists__field_named($field_name)){
      my %field  = $self->_field_named($field_name);
      #warn("field_named=".$field{$field_name}."\n");
       my @fields = $self->fields();
       return $fields[$field{$field_name}]->value();
   }
   else {
     return undef;
   }
}

sub field_named {

   my $self= shift;
   my ($field_name) = @_;
   # warn("1 in field_named=".$field_name."\n");
   # my $table_name   = "";
   # $table_name      = $self->table()->name()
      # if ($self->table->name());
   # $table_name      = $self->table()->alias()
      # if ($self->table->alias());
      # 
   # $field_name= $table_name.".".$field_name
      # if ($table_name);
   # warn("field_named=".$field_name."\n");
   if($self->exists__field_named($field_name)){
      my %field  = $self->_field_named($field_name);
      #warn("field_named=".$field{$field_name}."\n");
       my @fields = $self->fields();
       return $fields[$field{$field_name}];
   }
   else {# then it is just a staic value
     return $field_name;
   }
}

sub validate_type {
   my $self = shift;
   my ($type) = @_;
   # Check $opt
   if (!exists(Constants::SQL::CLAUSE_TYPES->{$type})){
      die "error: DBIx::DA:::SQL->type must be a valid Clause Type!' ";
   }
   return;
}
sub validate_table {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   return 
     unless($opt);
     
   ref($opt) eq 'DBIx::DA::Table' || die("ERROR: DBIx::DA::SQL::table, must be a 'DBIx::DA::Table' object.");
   return 1;
}
sub add_dynamic_condition{
   my $self = shift;
   my ($opt) = @_;
   use Data::Dumper;
   if (ref($opt) eq 'DBIx::DA::Condition'){
     $self->push_dynamic_condition($opt);
   }
  
   if (ref($opt->{field}) eq "HASH") {
         #warn("add_dynamic_condition 3");

     $opt->{field}->{table}= $self->table()->name()
       unless($opt->{field}->{table});      
     $opt->{field}  = DBIx::DA::Field->new({name       =>$opt->{field}->{name},
                                     table_name =>$opt->{field}->{table}});
    
                            
   }    
    
   #warn("add_dynamic_condition ref filed =".ref($opt->{field})."\n");

   $opt->{field} = $self->field_named($opt->{field});
   die("ERROR: DBIx::DA::SQL::field, ".$opt->{field}." in ".ref($self)." does not exits.") 
     if (ref($opt->{field}) ne "DBIx::DA::Field");

  
   $opt->{param} = DBIx::DA::Param->new({value=>$opt->{param},_use_named_params=>$self->use_named_params()})
     if (ref($opt->{param}) ne "DBIx::DA::Param" and ref($opt->{param}) ne "ARRAY");

    
   if (ref($opt->{param}) eq "ARRAY"){
  
        my @params;
        foreach my $value (@{$opt->{param}}){
           push(@params,DBIx::DA::Param->new({value=>$value,_use_named_params=>$self->use_named_params()}));
        } 
       $opt->{params}=\@params;  
       delete($opt->{param});  
      #  warn "SQL add out opt =".Dumper($opt)."\n";
    }
 #warn "SQL add add_dynamic_condition 5\n";
  
    $self->push_dynamic_conditions( [DBIx::DA::Condition->new($opt)]);
   
}

sub add_hierachical_join {
   my $self  = shift;
   my ($opt) = @_;
        
      $self->push_hierarchical_joins( DBIx::DA::HierachicalJoin->new({start_value=>$opt->{start_value},
                                             start_field=> $self->field_named($opt->{start_field}),
                                           parent_field => $self->field_named($opt->{parent_field}),
                                           child_field  => $self->field_named($opt->{child_field})}));
}


sub add_static_join {
   my $self  = shift;
   my ($opt) = @_;
         
      $self->push_joins( DBIx::DA::Join->new({table_name=>$opt->{table_name},
                                              table_alias=>$opt->{table_alias},
                                           type =>$opt->{type},
                                           conditions=>[DBIx::DA::Condition->new($opt->{conditions})]}));
   
}
sub add_join{
   my $self  = shift;
   my ($opt) = @_;
   #Fwarn "SQL add condtion opt=".Dumper($opt);
   
   if (!$self->_initialized()){
           
      $self->push_joins( DBIx::DA::Join->new({table_name=>$opt->{table_name},
                                              table_alias=>$opt->{table_alias},
                                           type =>$opt->{type},
                                           conditions=>[DBIx::DA::Condition->new($opt->{conditions})]}));
   }
   else {
      $self->push_dynamic_joins( DBIx::DA::Join->new({table_name=>$opt->{table_name},
                                                     table_alias=>$opt->{table_alias},
                                           type =>$opt->{type},
                                           conditions=>[DBIx::DA::Condition->new($opt->{conditions})]}));
           
   }
   
}
sub add_condition{
   my $self = shift;
   my ($opt) = @_;
   use Data::Dumper;
   
   #warn "SQL add condtion opt=".Dumper($opt);
   
   die("ERROR: DBIx::DA::SQL::add_condition field, is empyt or or does not esits in ".ref($self)."!") 
       unless($opt->{field});
   
   $opt->{field} = $self->field_named($opt->{field});
   die("ERROR: DBIx::DA::SQL::add_condition field, ".$opt->{field}." in ".ref($self)." does not exits.") 
     if (ref($opt->{field}) ne "DBIx::DA::Field");
   
  
     
   $opt->{param} = DBIx::DA::Param->new({value=>$opt->{param},_use_named_params=>$self->use_named_params()})
     if (ref($opt->{param}) ne "DBIx::DA::Param" and ref($opt->{param}) ne "ARRAY");
     
   
   if (ref($opt->{param}) eq "ARRAY"){
        my @params;
        foreach my $value (@{$opt->{param}}){
           push(@params,DBIx::DA::Param->new({value=>$value,_use_named_params=>$self->use_named_params()}));
        } 
       $opt->{params}=\@params; 
       delete($opt->{param});  
    }
    $self->push_conditions( [DBIx::DA::Condition->new($opt)]);
    
   
}
 sub validate_dynamic_conditions {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   use Data::Dumper;
    #warn("in validate_dynamic_conditions".Dumper($opt)."\n\n");
    ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::SQL::dynamic_conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
   foreach my $condition (@{$opt}){
      ref($condition) eq 'DBIx::DA::Condition' || die("ERROR: DBIx::DA::SQL::dynamic_conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
      $condition->_type(Constants::SQL::WHERE);
      $condition->_parent(\$self);
   }
   return;
}
sub validate_conditions {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   return
     unless($opt);
     
   ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::SQL::conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
   foreach my $condition (@{$opt}){
      ref($condition) eq 'DBIx::DA::Condition' || die("ERROR: DBIx::DA::SQL::conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
      $condition->_type(Constants::SQL::WHERE);
      $condition->_parent(\$self);
   }
   return;
}

sub validate_fields {
   my $self = shift;
   my ($opt) = @_;
  
   return 1
     unless($opt);
  
  use Data::Dumper;
 
   my $table_name ;
  
   if ($self->table()){
     $table_name=$self->table()->name;
   }
   
   if (ref($opt) eq 'ARRAY'){
       foreach my $field (@{$opt}){
          if (ref($field) ne 'DBIx::DA::Field'){
             die("ERROR: DBIx::DA::SQL::fields, must be a 'DBIx::DA::Field' object.");
          }
           #warn("validate_fields field name=".$field->name);
          $field->table_name($table_name)
             unless($field->table_name());
          # $field->alias($field->table_name()."_".$field->name())
             # if ($self->table->name() ne $field->table_name() and (!$field->alias()));
             # 
          $self->_field_exists($field);
          if ($field->table_name() ne $self->table()->name()){
               $field->no_insert(1);
               $field->no_update(1);

          }          
       }
       return;
   }
   elsif( ref($opt) ne 'DBIx::DA::Field' ){
      die("ERROR: DBIx::DA::SQL::fields, must be 'DBIx::DA::Field' object.");
   }
   
   $opt->table_name($table_name)
      unless($opt->table_name());
   # $opt->alias($opt->table_name()."_".$opt->name())
      # if ($self->table->name() ne $opt->table_name() and (!$opt->alias()));
          # 
   $self->_field_exists($opt);
   if ($self->table()){
          
      if ($opt->table_name() ne $self->table()->name()){
               $opt->no_insert(1);
               $opt->no_update(1);

          }
   }      
   return;
}
sub validate_joins {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
  
   (ref($opt) eq 'DBIx::DA::Join' || ref($opt) eq 'DBIx::DA::HierachicalJoin' ) || die("ERROR: QuerySQL::joins, must be 'DBIx::DA::Join' object.");
   $opt->_parent(\$self);
   return 1;
}
sub validate_dynamic_joins {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
  
   ref($opt) eq 'DBIx::DA::Join' || die("ERROR: QuerySQL::dynamic_joins, must be 'DBIx::DA::Join' object.");
   $opt->_parent(\$self);
   return 1;
}
sub validate_having {
   my  $self = shift;
   my ($opt) = @_;
   return
     unless($opt);
  
   # Check $opt
   ref($opt) eq 'DBIx::DA::Having' || die("ERROR: QuerySQL::havings, must be 'DBIx::DA::Having' object.");
   $opt->_parent(\$self);
   return 1;
}

sub validate__params {
   my  $self = shift;
   my ($opt) = @_;
   # Check $opt
  #warn("validate__param".Dumper($opt));
}
sub validate_where {
   my  $self = shift;
   my ($opt) = @_;
   # Check $opt
   return
     if ($opt eq " " );
   ref($opt) eq 'DBIx::DA::Where' || die("ERROR: QuerySQL::where, must be 'DBIx::DA::Where' object.");
   $opt->_parent(\$self);
   return;
}
sub validate_returning {
   my  $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'DBIx::DA::Returning' || die("ERROR: QuerySQL::returning, must be 'DBIx::DA::Returning' object.");
   $opt->_parent(\$self);
   return;
}
sub validate_group_by {
   my  $self = shift;
   my ($opt) = @_;
   # Check $opt
   return 
     unless($opt);
     
   ref($opt) eq 'DBIx::DA::Group_by' || die("ERROR: QuerySQL::group_by, must be 'DBIx::DA::Group_by' object.");
   return 1;
}

sub _only_fields {

  my $self = shift;
  
  my @fields;
  foreach my $show_field ($self->only_fields()){
    push(@fields,$self->field_named($show_field));
  }
  return @fields;
}

sub set_field_values {
  my $self = shift;
  my ($opt)= @_;
  
  
  ref($opt) eq 'ARRAY' || die("ERROR: QuerySQL::set_field_values, must be an array of hashes.");
   
  foreach my $field (@{$opt}){
      
      my $new_value = $self->field_named($field->{name});
      die("ERROR: ". $self."::set_field_values, field='".$field->{name}."' not found in ".ref($opt))
        unless($new_value);
      next
       if ($new_value->is_identity());
       
      $new_value->value($field->{value});
      
     
  }
           
}
sub sql {
   my $self = shift;
   $self->_params([]);
   $self->returning([]);
   my $sql = "";
   $sql = $self->_insert()
     if ($self->_operation() eq Constants::DA::CREATE);
   $sql = $self->_select()
     if ($self->_operation() eq Constants::DA::RETRIEVE);
   $sql = $self->_update()
     if ($self->_operation() eq Constants::DA::UPDATE);
   $sql = $self->_delete()
     if ($self->_operation() eq Constants::DA::DELETE);
   # $sql .= $self->_from_clause()."\n";
   # $sql .= $self->_join_clause()."\n";
   # $sql .= $self->_where_clause()."\n";
   # $sql .= $self->_group_by_clause()."\n";
   # $sql .= $self->_having_clause()."\n";
   # $sql .= $self->_order_by_clause()."\n";
   return $sql;
}
sub _update {
   my $self = shift;
   my ($container) = @_;
   my $delimiter = "";
   my $expressions ="";
   $self->_params([]);
   my @fields_to_change = $self->fields();
   if ($container) {
     @fields_to_change=();
     
     foreach my $key (keys(%{$container})){
             
       my $field = $self->field_named($key);
       next
         unless $field;
       $field->value($container->{$key});
       push(@fields_to_change,$field);
     }          
   }
   
   foreach my $field (@fields_to_change){
      unless($field->no_update() or $field->is_identity()){
          $expressions .= $delimiter
                         .$field->name()
                         ." = ";
          if ($field->value() eq 'sysdate' ){
            $expressions.= "sysdate";
          }
          else {
            $self->push__params($field);
           
            if ($self->use_named_params()){
               $expressions.= " :p_".$field->name();
            }
            else {
               $expressions.=" ? ";
            }
         }
         $delimiter=", ";
      }
   }
   
   my $sql=Constants::SQL::UPDATE." ".$self->table->name()
                    ." SET "
                    .$expressions;
  # if ($self->joins()){
      # $sql.=$self->_join_clause();
  # }
  #warn("where clause next");
  if ($self->conditions() || $self->dynamic_conditions()){
  
     $sql.=$self->_where_clause()."\n";;
  }
  elsif(!$self->can_global_update()){
     die("ERROR: QuerySQL::upadate, Attempt to Update on a table without a 'where' clause when enable_global_update is not enabled!");
  }
  if ($self->returning()){
      $sql.=$self->_returning_clause();
  }
  return $sql;
}

sub _delete {
   my $self = shift;

   my $sql=Constants::SQL::DELETE." ".$self->table->name();

   if ($self->conditions() || $self->dynamic_conditions()){
        $sql.=$self->_where_clause()."\n";;
   }
   elsif(!$self->can_global_delete()){
      die("ERROR: QuerySQL::delete, Attempt to Delete on a table without a 'where' clause when enable_global_delete is not enabled!");
   }
   if ($self->returning()){
      $sql.=$self->_returning_clause();
   }
   
   #warn("Delete sql=".$sql);
   return $sql;
}

sub _insert {
   my $self = shift;
   my ($container) = @_;
   my $delimiter = "";
   my $field_str = "";
   my $value_str = "";
   my @fields_to_insert = $self->fields();
   my $sql= Constants::SQL::INSERT." INTO ".$self->table()->name();
   #$container->isa();
   

   if (ref($container) ne "HASH" ) {#insert with select
      foreach my $key ($self->_only_fields){
        my $field = $self->field_named($key);
        next
          unless $field;
        
        $field_str.= $delimiter
                  .$field->name();
        $delimiter=", ";
      }
      $sql.= " ("
            .$field_str
            ." ) "
            .$container->_select();
           
             foreach my $sub_param ($container->dynamic_conditions()){
                         $self->push__params($sub_param->param());  
             }
   }
   else {
           
     @fields_to_insert=();
     
     foreach my $key (keys(%{$container})){
     
       my $field = $self->field_named($key);
       next
         unless $field;
       use Data::Dumper;
       
       #warn("DA::SQL insert key=".$key); 
         #warn("DA::SQL insert field isa =".ref($field));      
       $field->value($container->{$key});
       push(@fields_to_insert,$field);
     }          
   
     foreach my $field (@fields_to_insert){
      
      unless($field->no_insert()){
         $field_str.= $delimiter
                   .$field->name();
         $value_str.= $delimiter;
         if ($field->is_identity() and $field->sequence() ){
            $value_str.= $field->sequence().".nextval";
            $self->returning(DBIx::DA::Returning->new({params=>[DBIx::DA::Param->new({name=>$field->name(),value=>\$field})]}));
         }
         elsif ($field->value() eq 'sysdate' ){
            $value_str.= "sysdate";
         }
         else {
            $self->push__params($field);
            #warn("insert param push");
            if ($self->use_named_params()){
               $value_str.= " :p_".$field->name();
            }
            else {
               $value_str.=" ? ";
            }
         }
         $delimiter=", ";
      }
     }
   
     $sql.= " ("
                        .$field_str
                        ." ) VALUES ("
                        .$value_str
                        .")";
   
   
     if ($self->returning()){
        $sql.=$self->_returning_clause();
     }
   }
   return $sql;
}
sub _select {
   my $self = shift;
   my $sql  = Constants::SQL::SELECT." ";
   my $delimiter = "";
   $self->_params([]);
   #warn("\n\n\n Starting Select statement $self\n\n");
   $sql.=" DISTINCT "
     if ($self->distinct());
   
   my @fields = $self->_only_fields;
   
   @fields = $self->fields()
     unless(scalar(@fields));
     
   foreach my $field (@fields){
      my $field_str = "";
       #warn("\n\n\n ref field $field x".ref($field)."x\n\n");
      unless  ( ref($field)){
        $field_str = "'$field'";
      }    
      else {
      #warn("filed=".$field->name()."\n");
      unless($field->no_select()){
         $field_str = $field->sql();
         if ($field->aggregate()){  #some sort of function on this
            $field_str = $field->aggregate()
                         ."($field_str)";
         }
         if ($field->function()){
           $field_str  = $field->function()->sql();
            
         }
         #warn($field->name().",".$field->alias()."\n");
         if ($field->alias()){
            $field_str.= " AS ". $field->alias();
         }
         
         
      }
      
      }
      $sql.=  $delimiter
              .$field_str;
      $delimiter = ", ";
   }
  
   $sql.=$self->_from_clause();
   
   
   #warn("select". );
   if ($self->joins() or $self->dynamic_joins()){
      $sql.=$self->_join_clause();
   }
  
  
   if ($self->conditions() || $self->dynamic_conditions()){
  
     $sql.=$self->_where_clause()."\n";;
   }
   
   if ($self->group_by()){
     $sql .= $self->_group_by_clause()."\n";
   }
   if ($self->having()){
      $sql .= $self->_having_clause()."\n";
   }
     if ($self->hierarchical_joins()){
      $sql.=$self->_hierarchical_clause();
   }
   if ($self->order_bys()){
      $sql .= $self->_order_by_clause()."\n";
   }
   
 
  
   #warn("\n\n\n Finished select with SQL=".$sql."\n");
   #$self->only_fields([]);#lasts only 1 select
   return($sql);
}
sub _from_clause {
   my $self = shift;
   return " FROM ".$self->table()->sql();
}


sub _where_clause {

   my $self = shift;
   my $sql = " WHERE ";
   my $add_logic = 0;
   #warn("Where clause ".ref($self)." \n");
   my $condition_count = scalar($self->conditions());
  
   foreach my $condition ($self->conditions()){
  #        warn("Where clause 2\n");
   #        warn("\nwhere sql 2=".$add_logic);
           
      if ($add_logic and $add_logic<$condition_count){
         if ($condition->logic()){
            $sql.=$condition->logic();
         }
         else {
            die("ERROR: DBIx::DA::SQL::where, You must hve a 'logic' attribute for two or more conttions.");
         }
      }
    # warn("Where clause 2d\n");
      $sql.=$condition->sql();
     #warn("Where clause 25d sql=".$sql."\n");
      $add_logic++;
   }
   
   if ($self->dynamic_conditions()){
 #        warn("Where clause 3\n");
     $sql.= Constants::SQL::AND
       if ($self->conditions());
     $add_logic = 0;
     $condition_count = scalar($self->dynamic_conditions());
 
     foreach my $condition ($self->dynamic_conditions()){
        #warn("\nwhere $condition 2=".$condition);
        if ($add_logic and $add_logic<$condition_count){
              #   warn("\nwhere sql 2a=".$add_logic);
           if ($condition->logic()){
               
               #     warn("\nwhere sql 2b=".$add_logic);
              $sql.=$condition->logic();
           }
           else {
             die("ERROR: ".$self."->DBIx::DA::SQL::where, You must hve a 'logic' attribute for two or more dynamic conttions.");
           }
        }
        # warn("Where clause ".Dumper($condition)." 4\n");
        $sql.=$condition->sql();
        # warn("Where clause 5\n");
        $add_logic++;
     }
     # warn("Where clause out sql=".$sql."\n");
   } 
   return $sql;
       
        
}
sub _hierarchical_clause {
   my $self = shift;
   my $sql  = " ";
   # foreach my $join ($self->joins()){
      # $sql.= $join->sql();
   # }
   foreach my $join ($self->hierarchical_joins()){
      $sql.= $join->sql();
   }
   return $sql;
}
sub _join_clause {
   my $self = shift;
   my $sql  = " ";
   foreach my $join ($self->joins()){
      $sql.= $join->sql();
   }
   foreach my $join ($self->dynamic_joins()){
      $sql.= $join->sql();
   }
   return $sql;
}
sub _order_by_clause {
   my $self = shift;
   my $sql  = " ORDER BY ";
   my $delimiter = "";
   foreach my $order_by ($self->order_bys()){
      $sql.= $delimiter
             .$order_by->sql();
      $delimiter = ", ";
   }
   return $sql;
}
# sub returning_clause {
   # my $self = shift;
   # return $self->returning_clause->sql();
# }
sub _having_clause {
   my $self = shift;
   return $self->having->sql()
     if $self->having();
}
sub _group_by_clause {
   my $self = shift;
   return $self->group_by()->sql()
      if $self->group_by();
}
# sub _where_clause {
   # my $self = shift;
   # return $self->where->sql()
    # if $self->where();
# }
sub _returning_clause {
   my $self = shift;
   return $self->returning->sql()
    if $self->returning();
}
{
package #hide from pause
 DBIx::DA::Table;
 use base qw(Orignal);
 use Carp();
 use warnings;
 use strict;
 DBIx::DA::Table->attributes({SCALARS =>[qw(name alias )],
                              });
 sub validate_name {
   my  $self = shift;
   my ($opt) = @_;
   # Check $opt
   die("ERROR: DBIx::DA::Table::name, must have a value!")
      if (!$opt);
 }
 sub sql {
   my $self = shift;
   if ($self->alias()){
      return  $self->name()." ". $self->alias();
   }
   else {
      return $self->name();
   }
 }
}
{
package
 DBIx::DA::Function;
 use base qw(Orignal);
 use Carp();
 use warnings;
 use strict;
 DBIx::DA::Function->attributes({SCALARS =>[qw(name)],
                                 ARRAYS  =>[qw(options)],
                                });
 
 # sub validate_options {
   # my  $self = shift;
   # my ($options) = @_;
   # # Check $opt
              # 
   # # if (!exists(Constants::SQL::FUNCTIONS->{$name })){
      # # die "error: DBIx::DA::Function '$name' must be a valid Function!' ";
   # # }
   # # 
   # return;
 # }
 
 sub validate_name {
   my  $self = shift;
   my ($name ) = @_;
   # Check $opt
   return
       if undef;
  if (!exists(Constants::SQL::FUNCTIONS->{$name})){
      die "error: DBIx::DA:::Field::name '$name' must be a valid Function!' ";
   }
   return;
 }
 
 sub sql {
   
   my $self = shift;
   my $delimiter = "";
   my $field_str = "";
   use Data::Dumper;
   foreach my $opt ($self->options()){
  
     if ((ref($opt) eq 'DBIx::DA::Field') or (ref($opt) eq 'DBIx::DA::Function')){
       $field_str.=$delimiter.$opt->sql();      
     }
     else {
       $field_str.=$delimiter.$opt;      
     }
     $delimiter = ",";
   }
   return $self->name()."($field_str)";            
   
 }
}
{
package
 DBIx::DA::Field;
 use base qw(Orignal );
 use Carp();
 use warnings;
 use strict;
 DBIx::DA::Field->attributes({SCALARS =>[qw(value
                                            name
                                            type
                                            alias
                                            source
                                            aggregate
                                            function
                                            no_select
                                            no_insert
                                            no_update
                                            returning
                                            results
                                            send_null
                                            is_unique
                                            is_identity
                                            sequence
                                            table_name )],
                              });
 sub sql {
   my $self = shift;
   my $sql  = $self->name();
   return $sql
     if ($self->table_name() eq Constants::DA::NONE);

   $sql = $self->table_name()
            ."."
            .$sql;
            

   return $sql;
 }
 sub validate_name {
   my  $self = shift;
   my ($opt) = @_;
   # Check $opt
   die("ERROR: DBIx::DA::Field::name, must have a value!")
      if (!$opt);
   return;
 }
 
 # sub validate_table_name {
   # my  $self = shift;
   # my ($opt) = @_;
   # # Check $opt
   # die("ERROR: DBIx::DA::Field::table_name, must have a value!")
      # if (!$opt);
   # return;
 # }
 
 sub validate_function {
   my  $self = shift;
   my ($opt ) = @_;
   return
       if undef;  
   ref($opt) eq 'DBIx::DA::Function' || die("ERROR: DBIx::DA::Field::function, must be 'DBIx::DA::Function' object.");
   
   return 1;    
   
 }
 sub validate_aggregate {
   my  $self = shift;
   my ($aggregate ) = @_;
   # Check $opt
   return
       if undef;
   if (!exists(Constants::SQL::AGGREGATES->{$aggregate })){
      die "error: DBIx::DA:::Field::aggregate '$aggregate' must be a valid Aggregate!' ";
   }
   return;
 }
}

{
package
 DBIx::DA::Param;
 use base qw(Orignal);
 use Carp();
 use warnings;
 use strict;
 DBIx::DA::Param->attributes({SCALARS =>[qw(value
                                            name
                                            type
                                            alias
                                            aggregate
                                            send_null
                                            _use_named_params)],
                              });
 sub sql {
   my $self = shift;
   #warn("Param name=".ref($self)."\n");
   #warn("Param value=".$self->value()."\n");
   
   # my $ = $self->_parent();
   # my $clause_ref = $$condition_ref->_parent();
   # my $da_ref = $$clause_ref->_parent();
   # $$self->_parent()->push__params($self);
   if ($self->_use_named_params()){
      return " :p_".$self->name();
   }
   else {
      return " ? ";
   }
 }
 sub validate_valuex {
   my  $self = shift;
   my ($opt) = @_;
   # Check $opt
   die("ERROR: DBIx::DA::Param::value, must have a value!")
      if (!$opt);
   return 1;
 }
 sub validate_aggregate {
   my  $self = shift;
   my ($aggregate ) = @_;
   # Check $opt
   return
       if undef;
   if (!exists(Constants::SQL::AGGREGATES->{$aggregate })){
      die "error: DBIx::DA:::Field::aggregate must be a valid Aggregate!' ";
   }
   return 1;
 }
}
{
package
 DBIx::DA::Join;
 use base qw(Orignal );
 use Carp();
 use warnings;
 use strict;
 DBIx::DA::Join->attributes({SCALARS =>[qw(table_name table_alias type _parent )],
                            ARRAYS  =>[qw(conditions)]});
 sub validate_table_name {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   $opt = $opt->table()->name()
     if (ref($opt) eq 'DBIx::DA::Table');
         
   return 1;
   
 }
 sub validate_conditions {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::Join::conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
   foreach my $condition (@{$opt}){
      ref($condition) eq 'DBIx::DA::Condition' || die("ERROR: DBIx::DA::Join::conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
      $condition->_type(Constants::SQL::JOIN);
      $condition->_parent(\$self);
   }
   return 1;
}
sub validate_type {
   my $self = shift;
   my ($type)=@_;
   if (!exists(Constants::SQL::JOINS->{$type})){
      die "error: DBIx::DA:::Join->type must be a valid Join Type!' ";
   }
   return 1;
}
sub validate_field {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'DBIx::DA::Field' || die("ERROR: DBIx::DA::Join::field, must be 'DBIx::DA::Field' object.");
   return 1;
}
sub sql {
   my $self = shift;
   
   #warn("SQL join=".$self->type().",".$self->table_name());
   my $alias = $self->table_alias()?" ".$self->table_alias():"";
   
   my $sql =  $self->type()
              ." JOIN "
              .$self->table_name()
              .$alias
              ." ON ";
   my $add_logic = 0;
   my $condition_count = scalar($self->conditions());
   foreach my $condition ($self->conditions()){
      if ($add_logic and $add_logic<$condition_count){
         if ($condition->logic()){
            $sql.=$condition->logic();
         }
         else {
            die("ERROR: DBIx::DA::Join::sql, You must hve a 'logic' attribute for two or more conitions ona JOIN.");
         }
      }
      $sql.=$condition->sql();
      $add_logic++;
   }
   return $sql;
}
}

{
package
 DBIx::DA::HierachicalJoin;
 use base qw(Orignal );
 use Carp();
 use warnings;
 use strict;
 DBIx::DA::HierachicalJoin->attributes({SCALARS =>[qw(start_value start_field parent_field child_field _parent )],
                            });

sub validate_start_field {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'DBIx::DA::Field' || die("ERROR: DBIx::DA::HierachicalJoin::start_field, must be 'DBIx::DA::Field' object.");
   return 1;
}

sub validate_parent_field {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'DBIx::DA::Field' || die("ERROR: DBIx::DA::HierachicalJoin::parent_field, must be 'DBIx::DA::Field' object.");
   return 1;
}

sub validate_child_field {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'DBIx::DA::Field' || die("ERROR: DBIx::DA::HierachicalJoin::child_field, must be 'DBIx::DA::Field' object.");
   return 1;
}
sub sql {
   my $self = shift;
   
   my $connector = ' = '.$self->start_value();
   $connector = Constants::SQL::IS_NULL
     if (uc($self->start_value()) eq Constants::SQL::NULL);
   
   my $sql = " START WITH "
             .$self->start_field->sql()
             .$connector
             ." CONNECT BY PRIOR "
             .$self->child_field->sql()
              ." = "
             .$self->parent_field->sql();
             
   return $sql;
}
}

{
package
DBIx::DA::Condition;
use base qw(Orignal );
DBIx::DA::Condition->attributes({SCALARS =>[qw(field operator param logic parenthes _type _parent)],
                                 ARRAYS  =>[qw(params)],
                              });
sub validate_parenthes {
   my $self = shift;
   my ($parenthes)=@_;
   #warn('validate_parenthes='.$parenthes);
   if (!exists(Constants::SQL::PARNES->{$parenthes})){
      die "error: DBIx::DA:::Condition->parenthes must be a valid Parenthes!' ";
   }
   return ;
}
sub validate_param{
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   return unless($opt);
   if ((ref($opt) ne 'DBIx::DA::Field') and (ref($opt) ne 'DBIx::DA::Param' ) and (ref($opt) ne 'DBIx::DA::SQL' )){
      die("ERROR: DBIx::DA::Condition::param, must be either a 'DBIx::DA::Field' or 'DBIx::DA::Param' object.");
   }
   # if (ref($opt) eq 'DBIx::DA::Param'){
      # $opt->_parent(\$self);
   # }
   return;
}
sub validate_params{
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   if ($opt){
      ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::Condition::params, must be an ARRAY of 'DBIx::DA::Field' or 'DBIx::DA::Param'  objects.");
      foreach my $param (@{$opt}){
          if ((ref($param) ne 'DBIx::DA::Field') and (ref($param) ne 'DBIx::DA::Param' )){
            die("ERROR: DBIx::DA::Condition::params, must be either a 'DBIx::DA::Field' or 'DBIx::DA::Param' object.");
         }
         # if (ref($param) eq 'DBIx::DA::Param' ){
            # $param->_parent(\$self);
         # }
      }
   }
   return;
}
sub validate_field{
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   use Data::Dumper;
   #warn("validate_field opt=".Dumper($opt));
   ref($opt) eq 'DBIx::DA::Field' || die("ERROR: DBIx::DA::Condition::field, must be 'DBIx::DA::Field' class not a ".Dumper($opt)."!");
   return;
}
sub validate_operator {
   my $self = shift;
   my ($operator)=@_;
   if (!exists(Constants::SQL::CONDITIONS->{$operator})){
      die "error: DBIx::DA:::Condition->operator '$operator' must be a valid Operator";
   }
   return;
}
sub validate_logic {
   my $self = shift;
   my ($logic)=@_;
   if (!exists(Constants::SQL::LOGIC->{$logic})){
      die "error: DBIx::DA:::Condition->logic must be a valid Logic Operator!' ";
   }
   return;
}

sub sql {
   my $self = shift;
   my $sql;
   my $parent=$self->_parent();
   use Data::Dumper;
   #warn("Condition sql\n");
   #warn("\SEL param ".Dumper($self->params())."\n");
   if ($self->parenthes() and $self->parenthes() eq Constants::SQL::OPEN_PARENS) {
      $sql.= Constants::SQL::OPEN_PARENS;
   }
   
   if ($self->_type() eq Constants::SQL::JOIN){
      $sql.=$self->field()->sql()
               .$self->operator
               .$self->param()->sql();
      #$$parent->push__params($self->param());
   }
   else {
       #warn("\nSEL operator ".$self->operator()."\n");
      if ($self->operator eq Constants::SQL::BETWEEN){
            my @params = $self->params();
            if (scalar(@params) != 2){
               die "error: DBIx::DA:::Condition BETWEEN must two params!' ";
            }
            $sql.=$self->field()->sql();
            $sql.=Constants::SQL::BETWEEN
           ." "
           .$params[0]->sql()
           .Constants::SQL::AND
           ." "
           .$params[1]->sql();
           $$parent->push__params(\@params);
      }
      elsif ($self->operator eq Constants::SQL::LIKE){
            $sql.=$self->field()->sql()
            .Constants::SQL::LIKE
            .$self->param()->sql();
             $$parent->push__params($self->param());
      }
      elsif ($self->operator eq Constants::SQL::IN || $self->operator eq Constants::SQL::NOT_IN){
         $sql.=$self->field()->sql()
               .$self->operator()
               .'(';
               my $comma = "";
               
               if ($self->param()){
                 my $param = $self->param();
                 #warn("param ref=".ref($param)."\n");
                  if ($param->value()->isa("DBIx::DA::SQL")){
                    my $sub_select  = $param->value();
                       
                    $sql.=$comma.$sub_select->_select(); 
#                    warn("\n\nsub_select=".$sub_select."\nsql=$sql\n");
                    foreach my $sub_param ($sub_select->dynamic_conditions()){
 #                     warn("dd=".$sub_param."\n");
 #                     warn("param=".$sub_param->param()->value."\n");
#                      warn("field=".$sub_param->param()->name."\n");
#                      warn("operatior=".$sub_param->param()->type."\n");
                      
                      #$$parent->push__params($sub_param);
                      # foreach my $value ($sub_param->params){
                              # warn("sub_param=".ref($sub_param->params)."\n");
#                         warn('Parent='.$$parent);
                         $$parent->push__params($sub_param->param());  
                      # }
                    } 
                    #$sub_select->dynamic_conditions([]);
                    # foreach my $sub_param ($param->value()->conditions()){
                      # $$parent->push__params($sub_param->params);  
                    # }                                            
                 }
               }       
               else {
                       
                 foreach my $param ($self->params()){
                    
                    $sql.=$comma.$param->sql();
                    $$parent->push__params($param);   
                    $comma = ",";
                 }
               }
               $sql.=')';
#               warn ("in sql=".$sql."\n");
               
      }
      elsif ($self->operator eq Constants::SQL::IS_NULL){
         $sql.=$self->field()->sql()
               .Constants::SQL::IS_NULL;
      }
      elsif ($self->operator eq Constants::SQL::IS_NOT_NULL){
         $sql.=$self->field()->sql()
               .Constants::SQL::IS_NOT_NULL;
      }
      elsif($self->params()) {
        my $values =[];
 #         warn("\nSEL params $self\n");
        foreach my $param ($self->params()) {               
           push(@{$values},$param->value);      
        }
        my $param = $self->shift_params();
        
        $param->value($values);
        
        $sql.=$self->field()->sql()
           .$self->operator
           .$param->sql();
           $self->param($param);
           $$parent->push__params($self->param());
      }
      else {
         $sql.=$self->field()->sql()
               .$self->operator
               .$self->param()->sql();
               $$parent->push__params($self->param());
               #warn("condtion param push");
      }
   }
   if ($self->parenthes() and $self->parenthes() eq Constants::SQL::CLOSE_PARENS) {
       $sql .=Constants::SQL::CLOSE_PARENS;
   }
  
   return $sql." ";
}
{
package
DBIx::DA::Group_by;
use base qw(Orignal );
DBIx::DA::Group_by->attributes({ARRAYS =>[qw(fields)],});
sub validate_fields {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::Group_by::fields, must be an ARRAY of 'DBIx::DA::Field' objects.");
   foreach my $field (@{$opt}){
      ref($field) eq 'DBIx::DA::Field' || die("ERROR: DBIx::DA::Group_by::fields, must be an ARRAY of 'DBIx::DA::Field' objects.");
   }
   return 1;
}
sub sql {
   my $self = shift;
   my $delimiter=" ";
   my $sql  = " GROUP BY ";
   foreach my $field ($self->fields()){
      my $field_str = $field->name();
      # if ($field->aggregate()){  #some sort of aggregate on this
         # $field_str = $field->aggregate()
                      # ."($field_str)";
      # }
      $sql.= $delimiter
             .$field_str;
      $delimiter = ", ";
   }
   return $sql;
}
}

# {
# package
# DBIx::DA::Transaction;
# use base qw(Orignal );
# DBIx::DA::Transaction->attributes({ARRAYS =>[qw(fields)],
                                   # HASHES =>[qw(transaction 
                                                # result)]
                                  # });
                                  # 
# }

# {
# package
# DBIx::DA::Where;
# use base qw(Orignal );
# DBIx::DA::Where->attributes({SCALARS =>['_parent'],
                            # });
# # sub validate_conditions {
   # # my $self = shift;
   # # my ($opt) = @_;
   # # # Check $opt
   # # ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::Where::conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
   # # foreach my $condition (@{$opt}){
      # # ref($condition) eq 'DBIx::DA::Condition' || die("ERROR: DBIx::DA::Where::conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
      # # $condition->_type(Constants::SQL::WHERE);
      # # $condition->_parent(\$self);
   # # }
   # # return;
# # }
# sub sql {
   # my $self = shift;
   # my $sql = " WHERE ";
   # my $add_logic = 0;
   # 
   
   # 
   # my $condition_count = scalar($self->conditions());
   # #warn("\nwhere sql 1=".$condition_count);
   # foreach my $condition ($self->conditions()){
           # #warn("\nwhere sql 2=".$add_logic);
      # if ($add_logic and $add_logic<$condition_count){
         # if ($condition->logic()){
            # $sql.=$condition->logic();
         # }
         # else {
            # $sql.= Constants::SQL::AND;
            # #die("ERROR: DBIx::DA::Where::sql, You must hve a 'logic' attribute for two or more conttions.");
         # }
      # }
      # $sql.=$condition->sql();
      # $add_logic++;
   # }
   # return $sql;
# }
# }
{
package
DBIx::DA::Having;
use base qw(Orignal );
DBIx::DA::Having->attributes({SCALARS=>[qw(_parent)],
                              ARRAYS  =>[qw(conditions)]});
sub validate_conditions {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::Having::conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
   foreach my $condition (@{$opt}){
      ref($condition) eq 'DBIx::DA::Condition' || die("ERROR: DBIx::DA::Having::conditions, must be an ARRAY of 'DBIx::DA::Condition' objects.");
      $condition->_type(Constants::SQL::HAVING);
      $condition->_parent(\$self);
   }
   return;
}
sub sql {
   my $self = shift;
   my $sql = " HAVING ";
   my $add_logic = 0;
   my $condition_count = scalar($self->conditions());
   foreach my $condition ($self->conditions()){
      if ($add_logic and $add_logic<$condition_count){
         if ($condition->logic()){
            $sql.=$condition->logic();
         }
         else {
            die("ERROR: DBIx::DA::Where::sql, You must hve a 'logic' attribute for two or more conttions.");
         }
      }
      $sql.=$condition->sql();
      $add_logic++;
   }
   return $sql;
}
}
{
package
DBIx::DA::Order_by;
use base qw(Orignal );
DBIx::DA::Order_by->attributes({SCALARS =>[qw(field type )]},);
sub validate_type {
   my $self = shift;
   my ($type)=@_;
   if (!exists(Constants::SQL::ORDER_BY->{$type})){
      die "error: DBIx::DA:::Order_By->type must be a valid Order By Type!' ";
   }
   return 1;
}
sub validate_field {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
    return 1
     unless($opt);
   ref($opt) eq 'DBIx::DA::Field' || die("ERROR: DBIx::DA::Order_by::field, must be 'DBIx::DA::Field' object.");
   return 1;
}
sub sql {
   my $self = shift;
   return $self->field()->sql()." ".$self->type();
}
}
{
package
DBIx::DA::Returning;
use base qw(Orignal );
DBIx::DA::Returning->attributes({SCALARS =>['_parent'],
                             ARRAYS  =>[qw(params)],
                              });
sub validate_conditions {
   my $self = shift;
   my ($opt) = @_;
   # Check $opt
   ref($opt) eq 'ARRAY' ||  die("ERROR: DBIx::DA::Returning::params, must be an ARRAY of 'DBIx::DA::Param' objects.");
   foreach my $field (@{$opt}){
      ref($field) eq 'DBIx::DA::Param' || die("ERROR: DBIx::DA::Returning::params, must be an ARRAY of 'DBIx::DA::Param' objects.");
      $field->_parent(\$self);
   }
   return;
}
sub sql {
   my $self = shift;
   my $delimiter = "";;
   my $field_str = "";
   my $param_str = "";
   my $da_ref = $self->_parent();
   foreach my $param ($self->params()){
      my $field_tmp = $param->name();
      if ($param->aggregate()){  #some sort of function on this
          $field_tmp .= $param->aggregate()
                       ."($field_tmp)";
      }
      
      $field_str .= $delimiter
                    .$field_tmp;
      my $param_tmp = "?";
      if ($$da_ref->use_named_params()){
        $param_tmp = " :p_".$param->name();
      }
      $param_str .= $delimiter
                    .$param_tmp;
      $delimiter =", ";
   }
   return " RETURNING "
          .$field_str
          ." INTO "
          .$param_str;
}
}
}
1;
