#!/bin/bash
export PYTHONWARNINGS="ignore:Unverified HTTPS request"

if  [[ ! "$(python3 -V)" =~ "Python 3" ]]; then
    echo "Make sure python3 is installed"
    exit 1
fi


# Check that this script is not run by root
if [ $(id -u) -eq 0 ]; then
    echo "This script needs to be run as a non-root user"
    exit 1
fi

# Check that we're in the right directory, i.e a git directory
if [ ! -d .git ]; then
    echo "This script needs to be run within the top level directory where .git/ exists"
    exit 1
fi

# Get variables of terminal for formatting
UNDERLINE='\e[4m'
NOFORMAT='\e[0m'

# Create Virtual Environment
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null 2>&1 && pwd)"
VENV_DIR=$SCRIPT_DIR/venv
if [ ! -d $VENV_DIR ]; then
    echo "Creating python virtual environment"
    python3 -m venv $VENV_DIR >/dev/null
fi
python3 -m venv $VENV_DIR
# Upgrade pip and install wheel in virtual environment
echo "Upgrading PIP and installing wheel in Virtual Environment"
source $VENV_DIR/bin/activate
$VENV_DIR/bin/pip3 install pip --upgrade >/dev/null
$VENV_DIR/bin/pip3 install wheel >/dev/null
deactivate

# Install python dependencies
echo "Installing Python dependencies into Virtual Environment"
source $VENV_DIR/bin/activate
$VENV_DIR/bin/pip3 install -r requirements.txt >/dev/null
deactivate

echo "Script finished successfully"
exit 0
