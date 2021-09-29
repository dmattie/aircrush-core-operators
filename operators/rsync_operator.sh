#!/bin/bash

UUID=$(cat /proc/sys/kernel/random/uuid)
echo $UUID
#TEMP=$(getopt -o dp:vh --long dev,publish:,verbose,help -- "$@")
TEMP=$(getopt -o h --long help -- "$@")


# Note the quotes around '$TEMP': they are essential!
eval set -- "$TEMP"

#default values
DEV=0
VERBOSE=
while true; do
    case "$1" in
      #  -d | --dev ) DEV=1; shift ;;
      #  -p | --publish ) PUBLISH="$2" ; shift 2;;
      #  -v | --verbose ) VERBOSE="-v" ; shift ;;
        -h | --help ) HELP=1; shift ;;
        -- ) if [ -n "$2" ]
            then
                SRC=$2
                if [ -n "$3" ]
                then
                    DEST=$3
                    if [ -n "$4" ]
                    then
                        ACCOUNT=$4
                        if [ -n "$5" ]
                        then
                            TIME_QUOTA=$5
                            if [ -n "$6" ]                            
                            then
                                shift 3
                                echo "Unexpected options: \"$@\" . exiting."
                                exit 1;
                            fi
                        fi
                    fi
                fi
            fi
            shift 2; break;;
        * ) break ;;
    esac
done
if (( $HELP ));then
    printf "\nUsage:\n"
    printf "\trsync_operator.sh [options] [--] <optstring> [USER@HOST:]SRC [USER@HOST:]DEST [ACCOUNT] [TIME_QUOTA]"
    printf "\n\nOptions:\n"
    printf "\t-h, --help\t\tdisplay this help\n"
    printf "Example:\n\trsync_operator.sh myuser@cedar.computecanada.ca:~/scratch/directory myuser@graham.computecanada.ca:~/scratch/directory/ def-myuser 1:25:00\n\n"
    
    exit 0
fi

#printf "SRC=${SRC}\nDEST=${DEST}\naccount=${ACCOUNT}\nTIME=${TIME_QUOTA}\nhelp=${HELP}\n"





printf "#!/bin/bash\n" > "/tmp/${UUID}.sh"
printf "#SBATCH --time=${TIME_QUOTA}\n" >> "/tmp/${UUID}.sh"
printf "#SBATCH --account=${ACCOUNT}\n" >> "/tmp/${UUID}.sh"
printf "#SBATCH --mem-per-cpu=1000M\n" >> "/tmp/${UUID}.sh"
printf "#SBATCH -e ${UUID}.err\n" >> "/tmp/${UUID}.sh"
printf "#SBATCH -o ${UUID}.out\n" >> "/tmp/${UUID}.sh"

printf "rsync -r ${SRC} ${DEST}\n\n" >> "/tmp/${UUID}.sh"
printf "if [[ \$? -eq 0 ]];then\n" >> "/tmp/${UUID}.sh"
printf '  echo "rsync completed successfully"' >> "/tmp/${UUID}.sh"
printf "\nelse\n" >> "/tmp/${UUID}.sh"
printf "  echo \"rsync failed with exit code:\$?\" \n" >> "/tmp/${UUID}.sh"
printf "fi\n\n" >> "/tmp/${UUID}.sh"
# util=~/projects/def-dmattie/crush/utilities
# echo 


sbatch "/tmp/${UUID}.sh"