#!/usr/bin/env bash

# This will download and start a local customized perfetto instance
# This instance shows all flows per default as this is quite important for overview reasons

# prepare folders
mkdir perfetto_webserver
cd perfetto_webserver || exit 1
rm -rf dist

# download and unzip
wget -N https://github.com/TrimVis/perfetto/releases/latest/download/ui_build.zip
unzip ui_build.zip

# start webserver
cd dist || exit 1
echo "Starting perfetto webserver at http://localhost:9000"
python -m http.server 9000
