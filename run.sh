#!/bin/bash

project_directory="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $project_directory

if [ ! -e .venv ];
then
    ./setup.sh
fi

tools/with_venv.sh python $project_directory/lib/run.py "$@"
