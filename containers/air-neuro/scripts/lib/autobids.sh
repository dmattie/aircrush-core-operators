#!/bin/bash

SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/lib/autobids/case_a.sh"
source "${SCRIPTPATH}/lib/autobids/case_b.sh"

export case_a_test
export case_a_autobids

export case_b_test
export case_b_autobids

export case_c_test
export case_c_autobids
