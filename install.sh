#!/usr/bin/env bash

cd "$(dirname "$0")"

# get submodules first
git submodule update --init --recursive

set -e

# be sure using python3.6 in your env

rm -rf _virtualenv
virtualenv _virtualenv
source _virtualenv/bin/activate

# The advantage of using this method, in contrary to just calling `pip install -r requirements.txt` several times,
# is that it can detect different versions of the same dependency and fail with a "Double requirement given"
# error message.
pip install $(cat requirements.txt $(find lib -name requirements.txt | sort) | sort | uniq | sed 's/ *== */==/g')
