package Constants::Condition;
use warnings;
use strict;
BEGIN {
  $Constants::Condition::VERSION = "0.01";
}
use constant SELECT =>'SELECT';
use constant INSERT =>'INSERT';
use constant UPDATE =>'UPDATE';
use constant DELETE =>'DELETE';
use constant IN          =>' IN ';
use constant BETWEEN     =>' BETWEEN' ;
use constant LIKE        =>' LIKE ';
use constant IS_NULL     =>' IS_NULL ';
use constant IS_NOT_NULL =>' IS_NOT_NULL ';
use constant AND         =>' AND ';
use constant OR          =>' OR ';
use constant JOIN        =>'JOIN';
use constant WHERE       =>'WHERE';
use constant HAVING       =>'HAVING';
use constant OPEN_PARENS =>' ( ';
use constant OPEN_PARENTHESES =>Constants::Condition::OPEN_PARENS;
use constant CLOSE_PARENS =>' ( ';
use constant CLOSE_PARENTHESES =>Constants::Condition::CLOSE_PARENS;
use constant OPERATION_TYPES =>{Constants::Condition::SELECT =>1,
                                Constants::Condition::INSERT =>1,
                                Constants::Condition::UPDATE =>1,
                                Constants::Condition::DELETE =>1};
use constant CLAUSE_TYPES=>Constants::Condition::OPERATION_TYPES;
use constant CONDITION_TYPES =>{Constants::Condition::JOIN  =>1,
                                Constants::Condition::WHERE =>1,
                                Constants::Condition::HAVING=>1};
use constant CONDITIONS =>{
             Constants::Condition::IN          =>1,
             Constants::Condition::BETWEEN     =>1,
             Constants::Condition::LIKE        =>1,
             Constants::Condition::IS_NULL     =>1,
             Constants::Condition::IS_NOT_NULL =>1,
             Constants::Condition::AND         =>1,
             Constants::Condition::OR          =>1,
             '='  =>1,
             '!=' =>1,
             '<>' =>1,
             '>'  =>1,
             '>=' =>1,
             '<'  =>1,
             '<=' =>1};
use constant LOGIC =>{Constants::Condition::AND=>1,
                      Constants::Condition::OR =>1};
use constant PARNES =>{Constants::Condition::OPEN_PARENS  =>1,
                       Constants::Condition::CLOSE_PARENS =>1};
 1;
