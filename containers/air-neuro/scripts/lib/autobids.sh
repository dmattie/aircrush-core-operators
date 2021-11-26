#!/bin/bash

SCRIPT=$( realpath $0 )
SCRIPTPATH=$( dirname $SCRIPT )
source "${SCRIPTPATH}/autobids/case_a.sh"
source "${SCRIPTPATH}/autobids/case_b.sh"

[[ $0 != $BASH_SOURCE ]] && echo "Script is being sourced"
echo "${BASH_SOURCE[${#BASH_SOURCE[@]} - 1]}"
export case_a_test
export case_a_autobids

export case_b_test
export case_b_autobids

export case_c_test
export case_c_autobids
