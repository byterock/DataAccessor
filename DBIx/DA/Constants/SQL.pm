package Constants::SQL;
use warnings;
use strict;
BEGIN {
  $Constants::SQL::VERSION = "0.01";
}
use constant SELECT =>'SELECT';
use constant INSERT =>'INSERT';
use constant UPDATE =>'UPDATE';
use constant DELETE =>'DELETE';
use constant IN          =>' IN ';
use constant NOT_IN      =>' NOT IN ';
use constant BETWEEN     =>' BETWEEN' ;
use constant LIKE        =>' LIKE ';
use constant IS_NULL     =>' IS NULL ';
use constant NULL        =>'NULL';
use constant IS_NOT_NULL =>' IS NOT NULL ';
use constant AND         =>' AND ';
use constant OR          =>' OR ';
use constant JOIN        =>'JOIN';
use constant WHERE       =>'WHERE';
use constant HAVING      =>'HAVING';
use constant OPEN_PARENS =>' ( ';
use constant OPEN_PARENTHESES =>Constants::SQL::OPEN_PARENS;
use constant CLOSE_PARENS =>' ) ';
use constant CLOSE_PARENTHESES =>Constants::SQL::CLOSE_PARENS;
use constant OPERATION_TYPES   =>{Constants::SQL::SELECT =>1,
                                  Constants::SQL::INSERT =>1,
                                  Constants::SQL::UPDATE =>1,
                                  Constants::SQL::DELETE =>1};
use constant CLAUSE_TYPES=>Constants::SQL::OPERATION_TYPES;
use constant CONDITION_TYPES =>{Constants::SQL::JOIN  =>1,
                                Constants::SQL::WHERE =>1,
                                Constants::SQL::HAVING=>1};
use constant CONDITIONS =>{
             Constants::SQL::IN          =>1,
             Constants::SQL::NOT_IN      =>1,
             Constants::SQL::BETWEEN     =>1,
             Constants::SQL::LIKE        =>1,
             Constants::SQL::IS_NULL     =>1,
             Constants::SQL::IS_NOT_NULL =>1,
             Constants::SQL::AND         =>1,
             Constants::SQL::OR          =>1,
             '='  =>1,
             '!=' =>1,
             '<>' =>1,
             '>'  =>1,
             '>=' =>1,
             '<'  =>1,
             '<=' =>1,
             'in' =>1};
use constant LOGIC =>{Constants::SQL::AND=>1,
                      Constants::SQL::OR =>1};
use constant PARNES=>{Constants::SQL::OPEN_PARENS  =>1,
                      Constants::SQL::CLOSE_PARENS =>1};

use constant JOIN        =>'JOIN';
use constant OUTER       =>'OUTER ';
use constant LEFT        =>'LEFT';
use constant LEFT_OUTER  =>'LEFT OUTER';
use constant RIGHT_OUTER =>'RIGHT OUTER';
use constant RIGHT       =>'RIGHT';

use constant FULL_OUTER  =>'FULL OUTER';
use constant INNER       =>'INNER';
use constant LEFT_INNER  =>'LEFT INNER';
use constant RIGHT_INNER =>'RIGHT INNER';
use constant FULL_INNER  =>'FULL INNER';
use constant CONNECT_BY  =>'CONNECT BY';
use constant CONNECT_BY_PRIOR =>'CONNECT BY PRIOR';
use constant START_WITH  =>'START WITH';
use constant JOINS =>{
             Constants::SQL::LEFT        =>1,
             Constants::SQL::RIGHT       =>1,
             Constants::SQL::OUTER       =>1,
             Constants::SQL::LEFT_OUTER  =>1,
             Constants::SQL::RIGHT_OUTER =>1,
             Constants::SQL::FULL_OUTER  =>1,
             Constants::SQL::INNER       =>1,
             Constants::SQL::LEFT_INNER  =>1,
             Constants::SQL::RIGHT_INNER =>1,
             Constants::SQL::CONNECT_BY  =>1,
             Constants::SQL::CONNECT_BY_PRIOR =>1,
             Constants::SQL::START_WITH  =>1,};
use constant AVG    =>'AVG';
use constant COUNT  =>'COUNT';
use constant FIRST  =>'FIRST';
use constant LAST   =>'LAST';
use constant MAX    =>'MAX';
use constant MIN    =>'MIN';
use constant SUM    =>'SUM';
use constant CONCAT =>'CONCAT';


use constant REQUIRED =>'R';
use constant OPTIONAL =>'O';
use constant NOW =>'sysdate';

use constant AGGREGATES =>{
             Constants::SQL::AVG   =>1,
             Constants::SQL::COUNT =>1,
             Constants::SQL::FIRST =>1,
             Constants::SQL::LAST  =>1,
             Constants::SQL::MAX   =>1,
             Constants::SQL::MIN   =>1,
             Constants::SQL::SUM   =>1,
            };


use constant FUNCTIONS =>{
              Constants::SQL::CONCAT=>{Constants::SQL::REQUIRED=>2,
                                       Constants::SQL::OPTIONAL=>0 
                                      },};

         
use constant ASC   =>' ASC ';
use constant DESC  =>' DESC ';
use constant ORDER_BY =>{
             Constants::SQL::ASC   =>1,
             Constants::SQL::DESC  =>1,};                       
                       
                       
 1;
