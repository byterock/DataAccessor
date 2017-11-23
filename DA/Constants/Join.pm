package Constants::Join;
use warnings;
use strict;
BEGIN {
  $Constants::Join::VERSION = "0.01";
}
use constant JOIN        =>'JOIN';
use constant OUTER       =>'OUTER ';
use constant OUTER_LEFT  =>'OUTER LEFT';
use constant OUTER_RIGHT =>'OUTER RIGHT';
use constant OUTER_FULL  =>'OUTER FULL';
use constant INNER       =>'INNER';
use constant INNER_LEFT  =>'INNER LEFT';
use constant INNER_RIGHT =>'INNER RIGHT';
use constant INNER_FULL  =>'INNER FULL';
use constant CONNECT_BY  =>'CONNECT BY';
use constant CONNECT_BY_PRIOR =>'CONNECT BY PRIOR';
use constant START_WITH  =>'START WITH';
use constant JOINS =>{
             Constants::Join::OUTER       =>1,
             Constants::Join::OUTER_LEFT  =>1,
             Constants::Join::OUTER_RIGHT =>1,
             Constants::Join::OUTER_FULL  =>1,
             Constants::Join::INNER       =>1,
             Constants::Join::INNER_LEFT  =>1,
             Constants::Join::INNER_RIGHT =>1,
             Constants::Join::CONNECT_BY  =>1,
             Constants::Join::CONNECT_BY_PRIOR =>1,
             Constants::Join::START_WITH  =>1,};
 1;
