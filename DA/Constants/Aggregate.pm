package Constants::Aggregates;
use warnings;
use strict;
BEGIN {
  $Constants::Aggregate::VERSION = "0.01";
}
use constant AVG   =>'AVG';
use constant COUNT =>'COUNT';
use constant FIRST =>'FIRST';
use constant LAST  =>'LAST';
use constant MAX   =>'MAX';
use constant MIN   =>'MIN';
use constant SUM   =>'SUM';
use constant AGGREGATESq =>{
             Constants::Aggregate::AVG   =>1,
             Constants::Aggregate::COUNT =>1,
             Constants::Aggregate::FIRST =>1,
             Constants::Aggregate::LAST  =>1,
             Constants::Aggregate::MAX   =>1,
             Constants::Aggregate::MIN   =>1,
             Constants::Aggregate::SUM   =>1,};
 1;
