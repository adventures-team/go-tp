package Project::TP::QMProfile;

use strict;
use warnings;

use parent 'MR::TP::QMProfile';
use MR::Queue;

my $oc_prefix = '/project/system';

$MR::Queue::Base::OC_PREFIX = $oc_prefix; ##no critic (Modules::RequireExplicitInclusion)
sub QM_OC_PREFIX { $oc_prefix }
sub TP_NAMESPACE { 'Project::TP::' }

sub get_all_jobs { [1122, 1123] }

1;
