package Constants::DA;
use warnings;
use strict;
BEGIN {
  $Constants::DA::VERSION = "0.01";
}

use constant NONE    =>'NONE';
use constant CREATE  =>'C';
use constant RETRIEVE=>'R';
use constant UPDATE  =>'U';
use constant DELETE  =>'D';

use constant OPERATION_TYPES =>{
             Constants::DA::CREATE   =>1,
             Constants::DA::RETRIEVE =>1,
             Constants::DA::UPDATE   =>1,
             Constants::DA::DELETE   =>1,};
 1;
